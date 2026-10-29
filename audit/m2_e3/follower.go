// E3 supplementary experiment: independently observed producer ACK, follower Fetch and consumer callback.
// This file does not modify the original go module or historical experiment sources.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"sort"
	"sync"
	"time"
)

var seeds = []string{"172.31.0.41:19092", "172.31.0.42:19092", "172.31.0.43:19092"}

type hooks struct {
	mu       sync.Mutex
	last     int32
	byOffset map[int64]int32
	batches  []map[string]interface{}
	fetches  []int32
}

func (h *hooks) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, part int32, metrics kgo.FetchBatchMetrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.last = meta.NodeID
	h.batches = append(h.batches, map[string]interface{}{"broker": meta.NodeID, "topic": topic, "partition": part})
}
func (h *hooks) OnFetchRecordBuffered(r *kgo.Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.byOffset[r.Offset] = h.last
}
func (h *hooks) OnBrokerRead(meta kgo.BrokerMetadata, key int16, n int, wait, read time.Duration, err error) {
	if key == 1 {
		h.mu.Lock()
		h.fetches = append(h.fetches, meta.NodeID)
		h.mu.Unlock()
	}
}
func emit(tag string, value interface{}) { b, _ := json.Marshal(value); fmt.Printf("%s %s\n", tag, b) }
func must(e error) {
	if e != nil {
		panic(e)
	}
}

type event struct {
	offset    int64
	partition int32
	key, id   string
	hw        int64
	observed  time.Time
}

func main() {
	topic := flag.String("topic", "", "fresh single-partition RF3 topic")
	trial := flag.Int("trial", 0, "trial 1..5")
	flag.Parse()
	if *topic == "" || *trial < 1 || *trial > 5 {
		panic("topic and trial=1..5 required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	adm, e := kgo.NewClient(kgo.SeedBrokers(seeds...))
	must(e)
	defer adm.Close()
	listing, e := kadm.NewClient(adm).ListTopics(ctx, *topic)
	must(e)
	detail, ok := listing[*topic]
	if !ok {
		panic("topic not found")
	}
	var parts = detail.Partitions.Sorted()
	if len(parts) != 1 {
		panic("expected one partition")
	}
	p := parts[0]
	if p.Err != nil {
		panic(p.Err)
	}
	if len(p.ISR) != 3 || len(p.Replicas) != 3 {
		panic(fmt.Sprintf("expected RF3 ISR3, got %+v", p))
	}
	followers := make([]int32, 0, 2)
	for _, id := range p.Replicas {
		if id != p.Leader {
			followers = append(followers, id)
		}
	}
	sort.Slice(followers, func(i, j int) bool { return followers[i] < followers[j] })
	if len(followers) != 2 {
		panic("expected two followers")
	}
	follower := followers[0]
	rack := fmt.Sprintf("rack-%d", follower)
	key := fmt.Sprintf("e3-%s-%d", *topic, *trial)
	id := key + "-unique"
	emit("TOPOLOGY", map[string]interface{}{"trial": *trial, "topic": *topic, "partition": 0, "leader": p.Leader, "leader_epoch": p.LeaderEpoch, "replicas": p.Replicas, "isr": p.ISR, "selected_follower": follower, "client_rack": rack, "bootstrap": seeds[0]})
	h := &hooks{last: -1, byOffset: make(map[int64]int32)}
	c, e := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.Rack(rack), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{*topic: {0: kgo.NewOffset().AtStart()}}), kgo.WithHooks(h), kgo.FetchMaxWait(150*time.Millisecond))
	must(e)
	defer c.Close()
	prod, e := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ProducerLinger(0), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()))
	must(e)
	defer prod.Close()
	events := make(chan event, 2)
	fail := make(chan error, 1)
	startPoll := make(chan struct{})
	go func() {
		close(startPoll)
		for ctx.Err() == nil {
			fetches := c.PollFetches(ctx)
			if es := fetches.Errors(); len(es) > 0 {
				for _, fe := range es {
					select {
					case fail <- fe.Err:
					default:
					}
				}
			}
			fetches.EachPartition(func(part kgo.FetchTopicPartition) {
				for _, r := range part.Records {
					if string(r.Value) == id {
						events <- event{offset: r.Offset, partition: r.Partition, key: string(r.Key), id: string(r.Value), hw: part.HighWatermark, observed: time.Now()}
						return
					}
				}
			})
		}
	}()
	<-startPoll
	time.Sleep(400 * time.Millisecond) // Warm up; still part of timed observation when listener not established.
	t0 := time.Now()
	record := &kgo.Record{Topic: *topic, Partition: 0, Key: []byte(key), Value: []byte(id)}
	result := prod.ProduceSync(ctx, record)
	ackTime := time.Now()
	must(result.FirstErr())
	var ev event
	select {
	case ev = <-events:
	case e = <-fail:
		panic(e)
	case <-ctx.Done():
		panic(errors.New("consumer callback timed out"))
	}
	ackMs := float64(ackTime.Sub(t0).Microseconds()) / 1000
	readMs := float64(ev.observed.Sub(t0).Microseconds()) / 1000
	h.mu.Lock()
	broker, exists := h.byOffset[ev.offset]
	batches := h.batches
	fetches := h.fetches
	h.mu.Unlock()
	leaderRead := broker == p.Leader
	followerRead := exists && broker == follower
	emit("MEASUREMENT", map[string]interface{}{"trial": *trial, "topic": *topic, "partition": ev.partition, "offset": ev.offset, "key": ev.key, "record_id": ev.id, "producer_offset": record.Offset, "producer_partition": record.Partition, "leader": p.Leader, "selected_follower": follower, "batch_serving_broker": broker, "batch_hook_recorded": exists, "follower_fetch_confirmed": followerRead, "leader_fetch_reported": leaderRead, "fetch_response_brokers": fetches, "fetch_batch_events": batches, "fetch_high_watermark": ev.hw, "ack_ms": ackMs, "send_to_callback_ms": readMs, "ack_to_callback_ms": float64(ev.observed.Sub(ackTime).Microseconds()) / 1000, "timing_source": "monotonic time.Time within one Go process", "send_utc": t0.UTC().Format(time.RFC3339Nano), "ack_utc": ackTime.UTC().Format(time.RFC3339Nano), "observed_utc": ev.observed.UTC().Format(time.RFC3339Nano)})
	if record.Offset != ev.offset || ev.partition != 0 || ev.key != key || ev.hw <= ev.offset {
		panic("id offset high watermark mismatch")
	}
	if !followerRead {
		fmt.Fprintln(os.Stderr, "FOLLOWER_NOT_PROVEN: batch broker did not match selected follower")
		os.Exit(3)
	}
}
