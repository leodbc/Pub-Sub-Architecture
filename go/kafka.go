package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaTopic = "probe"

type kafkaB struct {
	seeds  []string
	cl     *kgo.Client
	adm    *kadm.Client
	minISR int
	keyMode string
}

func newKafka(o Opts) (Backend, error) {
	if o.KeyMode != "fixed" && o.KeyMode != "id" { return nil, fmt.Errorf("key-mode inválido: %s", o.KeyMode) }
	seeds := strings.Split(o.Addrs, ",")
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ProducerLinger(0), // sem batching: mede a latência real de cada envio
		kgo.RecordDeliveryTimeout(o.Timeout),
	}
	switch o.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default:
		return nil, fmt.Errorf("acks inválido: %s (use all ou 1)", o.Acks)
	}
	if !o.Idem {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &kafkaB{seeds: seeds, cl: cl, adm: kadm.NewClient(cl), minISR: o.MinISR, keyMode: o.KeyMode}, nil
}

func (k *kafkaB) Setup(ctx context.Context, partitions, replicas int) error {
	v := strconv.Itoa(k.minISR)
	var err error
	for i := 0; i < 10; i++ { // após um Reset o tópico pode demorar a sumir
		_, err = k.adm.CreateTopic(ctx, int32(partitions), int16(replicas),
			map[string]*string{"min.insync.replicas": &v}, kafkaTopic)
		if err == nil || errors.Is(err, kerr.TopicAlreadyExists) {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}

func (k *kafkaB) Reset(ctx context.Context) error {
	_, err := k.adm.DeleteTopics(ctx, kafkaTopic)
	return err
}

func (k *kafkaB) Produce(ctx context.Context, id uint64, payload []byte) (string, error) {
	key := []byte("k")
	if k.keyMode == "id" { key = []byte(strconv.FormatUint(id, 10)) }
	rec := &kgo.Record{Topic: kafkaTopic, Key: key, Value: payload}
	if err := k.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return "", err
	}
	return fmt.Sprintf("p%d@%d", rec.Partition, rec.Offset), nil
}

func (k *kafkaB) Consume(ctx context.Context, idle time.Duration, fn func([]byte, string)) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.seeds...),
		kgo.ConsumeTopics(kafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	wait := idle * 3 // primeira leitura demora mais (metadados)
	for {
		c2, cancel := context.WithTimeout(ctx, wait)
		f := cl.PollFetches(c2)
		timedOut := c2.Err() != nil
		cancel()
		if f.NumRecords() == 0 && timedOut {
			return nil
		}
		f.EachRecord(func(r *kgo.Record) {
			fn(r.Value, fmt.Sprintf("p%d@%d", r.Partition, r.Offset))
		})
		wait = idle
	}
}

func (k *kafkaB) Close() { k.cl.Close() }
