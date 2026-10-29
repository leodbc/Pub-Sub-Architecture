// Complemento normativo Kafka E4: duas sequências separadas de 1..30.
// Não altera o sdtool nem substitui os testes históricos.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"sort"
	"strings"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	topic := flag.String("topic", "pedidos", "tópico normativo E1")
	flag.Parse()
	brokers := []string{"172.31.0.41:19092", "172.31.0.42:19092", "172.31.0.43:19092"}
	pub, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(0))
	must(err)
	defer pub.Close()
	type exp struct {
		scenario  string
		index     int
		partition int32
		offset    int64
		key       string
	}
	expected := make(map[string]exp)
	for _, scenario := range []string{"sem_chave", "mesma_chave"} {
		for i := 1; i <= 30; i++ {
			id := fmt.Sprintf("M2F4_%s_%02d", scenario, i)
			rec := &kgo.Record{Topic: *topic, Value: []byte(id)}
			key := "<nil>"
			if scenario == "mesma_chave" {
				rec.Key = []byte("mesma-chave-e4")
				key = "mesma-chave-e4"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			err := pub.ProduceSync(ctx, rec).FirstErr()
			cancel()
			must(err)
			expected[id] = exp{scenario, i, rec.Partition, rec.Offset, key}
			fmt.Printf("PRODUCE scenario=%s number=%02d partition=%d offset=%d key=%s value=%s\n", scenario, i, rec.Partition, rec.Offset, key, id)
		}
	}
	con, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(*topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	must(err)
	defer con.Close()
	observed := map[string]bool{}
	partGroups := map[string][]exp{}
	receiveOrder := map[string][]int{}
	deadline, stop := context.WithTimeout(context.Background(), 100*time.Second)
	defer stop()
	for len(observed) < len(expected) && deadline.Err() == nil {
		fetches := con.PollFetches(deadline)
		if es := fetches.Errors(); len(es) > 0 {
			for _, e := range es {
				fmt.Fprintf(os.Stderr, "FETCH_WARNING partition=%d err=%v\n", e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			id := string(r.Value)
			ex, exists := expected[id]
			if !exists {
				return
			}
			if observed[id] {
				panic("leitura duplicada " + id)
			}
			if r.Partition != ex.partition || r.Offset != ex.offset || string(r.Key) != strings.ReplaceAll(ex.key, "<nil>", "") {
				panic(fmt.Sprintf("inconsistência de leitura %s: part=%d offset=%d key=%q expected=%+v", id, r.Partition, r.Offset, r.Key, ex))
			}
			observed[id] = true
			partGroups[fmt.Sprintf("%s:%d", ex.scenario, ex.partition)] = append(partGroups[fmt.Sprintf("%s:%d", ex.scenario, ex.partition)], ex)
			receiveOrder[ex.scenario] = append(receiveOrder[ex.scenario], ex.index)
			fmt.Printf("CONSUME scenario=%s number=%02d partition=%d offset=%d key=%s value=%s\n", ex.scenario, ex.index, r.Partition, r.Offset, ex.key, id)
		})
	}
	if len(observed) != 60 {
		panic(fmt.Sprintf("E4 incompleto: %d/60", len(observed)))
	}
	for _, scenario := range []string{"sem_chave", "mesma_chave"} {
		partitions := map[int32]bool{}
		all := []exp{}
		for _, ex := range expected {
			if ex.scenario == scenario {
				partitions[ex.partition] = true
				all = append(all, ex)
			}
		}
		for p := range partitions {
			subset := []exp{}
			for _, ex := range all {
				if ex.partition == p {
					subset = append(subset, ex)
				}
			}
			sort.Slice(subset, func(i, j int) bool { return subset[i].offset < subset[j].offset })
			for i := 1; i < len(subset); i++ {
				if subset[i].index <= subset[i-1].index {
					panic(fmt.Sprintf("ordem de produção por partição alterada %s p%d", scenario, p))
				}
			}
			read := partGroups[fmt.Sprintf("%s:%d", scenario, p)]
			for i := 1; i < len(read); i++ {
				if read[i].offset <= read[i-1].offset {
					panic(fmt.Sprintf("ordem de leitura por partição alterada %s p%d", scenario, p))
				}
			}
		}
		if scenario == "mesma_chave" && len(partitions) != 1 {
			panic("chave única desviou de partição")
		}
		globallyAscending := true
		for i, n := range receiveOrder[scenario] {
			if n != i+1 {
				globallyAscending = false
				break
			}
		}
		fmt.Printf("E4_SUMMARY scenario=%s produced=30 consumed=30 distinct_partitions=%d per_partition_order=true observed_global_delivery_1_to_30=%t (observação, NÃO garantia global)\n", scenario, len(partitions), globallyAscending)
	}
	fmt.Println("E4_OK two_scenarios=2 messages_per_scenario=30 compare_offsets=true")
}
