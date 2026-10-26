package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsStream  = "PROBE"
	natsSubject = "probe.k"
)

type natsB struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	idem bool
}

func newNats(o Opts) (Backend, error) {
	nc, err := nats.Connect(o.Addrs,
		nats.Name("sdtool"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(200*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			fmt.Printf("[nats] desconectado: %v\n", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			fmt.Printf("[nats] reconectado em %s\n", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &natsB{nc: nc, js: js, idem: o.Idem}, nil
}

func (n *natsB) Setup(ctx context.Context, partitions, replicas int) error {
	// NATS não tem "partições": a unidade de replicação é o STREAM inteiro (grupo Raft próprio).
	_, err := n.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       natsStream,
		Subjects:   []string{"probe.>"},
		Replicas:   replicas,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: 2 * time.Minute, // janela de deduplicação por Nats-Msg-Id
	})
	return err
}

func (n *natsB) Reset(ctx context.Context) error {
	err := n.js.DeleteStream(ctx, natsStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return err
}

func (n *natsB) Produce(ctx context.Context, id uint64, payload []byte) (string, error) {
	var opts []jetstream.PublishOpt
	if n.idem {
		opts = append(opts, jetstream.WithMsgID(strconv.FormatUint(id, 10)))
	}
	// Publish síncrono: só retorna quando o líder do stream confirmou após commit por maioria (Raft).
	ack, err := n.js.Publish(ctx, natsSubject, payload, opts...)
	if err != nil {
		return "", err
	}
	pos := fmt.Sprintf("seq%d", ack.Sequence)
	if ack.Duplicate {
		pos += "(dup-descartado)"
	}
	return pos, nil
}

func (n *natsB) Consume(ctx context.Context, idle time.Duration, fn func([]byte, string)) error {
	cons, err := n.js.OrderedConsumer(ctx, natsStream, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return err
	}
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	cc, err := cons.Consume(func(m jetstream.Msg) {
		pos := "?"
		if md, err := m.Metadata(); err == nil {
			pos = fmt.Sprintf("seq%d", md.Sequence.Stream)
		}
		fn(m.Data(), pos)
		last.Store(time.Now().UnixNano())
	})
	if err != nil {
		return err
	}
	defer cc.Stop()
	for time.Since(time.Unix(0, last.Load())) < idle {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

func (n *natsB) Close() { n.nc.Close() }
