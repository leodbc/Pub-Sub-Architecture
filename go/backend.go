package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

// Opts reúne as opções comuns a todos os comandos.
type Opts struct {
	Sys     string
	Addrs   string
	Acks    string // kafka: "all" ou "1"
	KeyMode string // kafka: "fixed" ou "id"
	Idem    bool   // kafka: produtor idempotente | nats: Nats-Msg-Id (deduplicação)
	Timeout time.Duration
	MinISR  int
}

// Backend abstrai Kafka e NATS para que os MESMOS testes rodem nos dois.
type Backend interface {
	Setup(ctx context.Context, partitions, replicas int) error
	Reset(ctx context.Context) error
	// Produce envia e só retorna quando o broker confirmou. pos = offset/sequência.
	Produce(ctx context.Context, id uint64, payload []byte) (string, error)
	// Consume lê desde o início e para após 'idle' sem novas mensagens.
	Consume(ctx context.Context, idle time.Duration, fn func(payload []byte, pos string)) error
	Close()
}

func newBackend(o Opts) (Backend, error) {
	switch strings.ToLower(o.Sys) {
	case "kafka":
		return newKafka(o)
	case "nats":
		return newNats(o)
	}
	return nil, fmt.Errorf("sistema desconhecido: %s (use kafka ou nats)", o.Sys)
}

func defaultAddrs(sys string) string {
	if sys == "nats" {
		return "nats://172.33.0.51:4222,nats://172.33.0.52:4222,nats://172.33.0.53:4222"
	}
	return "172.31.0.41:19092,172.31.0.42:19092,172.31.0.43:19092"
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func meanStd(xs []float64) (float64, float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	m := s / float64(len(xs))
	if len(xs) < 2 {
		return m, 0
	}
	var v float64
	for _, x := range xs {
		v += (x - m) * (x - m)
	}
	return m, math.Sqrt(v / float64(len(xs)-1))
}
