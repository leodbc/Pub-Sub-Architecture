// sdtool: ferramenta única para os experimentos do Trabalho 2 (Kafka x NATS JetStream).
//
//	sdtool setup  --sys kafka|nats        cria tópico/stream replicado (RF=3)
//	sdtool reset  --sys ...               apaga tópico/stream
//	sdtool probe  --sys ...               produz mensagens numeradas continuamente e mede
//	                                      latência e janelas de indisponibilidade (E5/E6/E7/E8)
//	sdtool verify --sys ...               lê tudo e confere perdas, duplicatas e ordem
//	sdtool bench  --sys ...               N execuções: média e desvio da latência/vazão (E3)
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Msg struct {
	ID  uint64 `json:"id"`
	TS  int64  `json:"ts"`
	Pad string `json:"pad,omitempty"`
}

func mkPayload(id uint64, size int) []byte {
	b, _ := json.Marshal(Msg{ID: id, TS: time.Now().UnixNano(), Pad: strings.Repeat("x", max(0, size-40))})
	return b
}

func stamp() string { return time.Now().Format("15:04:05.000") }

func main() {
	if len(os.Args) < 2 {
		fmt.Println("uso: sdtool <setup|reset|probe|verify|bench> [flags]")
		os.Exit(1)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var o Opts
	fs.StringVar(&o.Sys, "sys", "kafka", "kafka | nats")
	fs.StringVar(&o.Addrs, "addrs", "", "endereços dos brokers (padrão do laboratório se vazio)")
	fs.StringVar(&o.Acks, "acks", "all", "kafka: all | 1")
	fs.BoolVar(&o.Idem, "idem", false, "idempotência (kafka: produtor idempotente; nats: Nats-Msg-Id)")
	fs.DurationVar(&o.Timeout, "timeout", 3*time.Second, "tempo máximo por envio")
	fs.IntVar(&o.MinISR, "minisr", 2, "kafka: min.insync.replicas do tópico")
	partitions := fs.Int("partitions", 1, "kafka: nº de partições")
	replicas := fs.Int("replicas", 3, "fator de replicação")
	count := fs.Int("count", 300, "probe: nº de mensagens")
	startID := fs.Uint64("start-id", 1, "probe: ID inicial exclusivo por rodada, evita colisões entre probes")
	interval := fs.Duration("interval", 100*time.Millisecond, "probe: intervalo entre mensagens")
	retry := fs.Int("retry", 0, "probe: retentativas da aplicação após erro")
	file := fs.String("file", "/tmp/acked.csv", "probe/verify: arquivo de confirmações")
	n := fs.Int("n", 200, "bench: mensagens por execução")
	runs := fs.Int("runs", 5, "bench: nº de execuções")
	size := fs.Int("size", 200, "tamanho aproximado da mensagem (bytes)")
	idle := fs.Duration("idle", 5*time.Second, "verify: encerra após este tempo sem mensagens")
	fs.Parse(os.Args[2:])
	if o.Addrs == "" {
		o.Addrs = defaultAddrs(o.Sys)
	}

	b, err := newBackend(o)
	if err != nil {
		fmt.Println("erro ao conectar:", err)
		os.Exit(1)
	}
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	switch cmd {
	case "setup":
		c, cc := context.WithTimeout(ctx, 30*time.Second)
		defer cc()
		check(b.Setup(c, *partitions, *replicas))
		fmt.Printf("[%s] criado (partições=%d, réplicas=%d)\n", o.Sys, *partitions, *replicas)
	case "reset":
		c, cc := context.WithTimeout(ctx, 30*time.Second)
		defer cc()
		if err := b.Reset(c); err != nil {
			fmt.Println("aviso:", err)
		}
		fmt.Printf("[%s] removido\n", o.Sys)
	case "probe":
		probe(b, o, *startID, *count, *interval, *retry, *file, *size)
	case "verify":
		verify(ctx, b, *file, *idle)
	case "bench":
		bench(b, o, *n, *runs, *size)
	default:
		fmt.Println("comando desconhecido:", cmd)
	}
}

func check(err error) {
	if err != nil {
		fmt.Println("erro:", err)
		os.Exit(1)
	}
}

func probe(b Backend, o Opts, firstID uint64, count int, interval time.Duration, retry int, file string, size int) {
	f, err := os.Create(file)
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "id,ok,lat_ms,t_ms,pos")

	var failStart time.Time
	var outages, lats []float64
	okN, failN := 0, 0

	for i := 0; i < count; i++ {
		id := firstID + uint64(i)
		payload := mkPayload(id, size)
		var pos string
		var err error
		var lat time.Duration
		for a := 0; a <= retry; a++ {
			ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
			t0 := time.Now()
			pos, err = b.Produce(ctx, id, payload)
			lat = time.Since(t0)
			cancel()
			if err == nil {
				break
			}
			fmt.Printf("%s id=%d tentativa=%d ERRO (%.0f ms): %v\n", stamp(), id, a+1, ms(lat), err)
			if failStart.IsZero() {
				failStart = time.Now().Add(-lat)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			failN++
			fmt.Fprintf(w, "%d,0,%.1f,%d,\n", id, ms(lat), time.Now().UnixMilli())
		} else {
			okN++
			lats = append(lats, ms(lat))
			if !failStart.IsZero() {
				d := time.Since(failStart)
				outages = append(outages, ms(d))
				fmt.Printf("%s >>> RECUPEROU: indisponível por %.0f ms\n", stamp(), ms(d))
				failStart = time.Time{}
			}
			fmt.Printf("%s id=%d OK %.1f ms pos=%s\n", stamp(), id, ms(lat), pos)
			fmt.Fprintf(w, "%d,1,%.1f,%d,%s\n", id, ms(lat), time.Now().UnixMilli(), pos)
		}
		w.Flush()
		time.Sleep(interval)
	}

	m, s := meanStd(lats)
	fmt.Println("\n===== RESUMO DO PROBE =====")
	fmt.Printf("confirmadas=%d  falhas=%d  latência média=%.1f ms (desvio %.1f)\n", okN, failN, m, s)
	for i, d := range outages {
		fmt.Printf("janela de indisponibilidade #%d: %.0f ms\n", i+1, d)
	}
	if !failStart.IsZero() {
		fmt.Println("ATENÇÃO: terminou ainda indisponível (sem quórum?)")
	}
	fmt.Println("Agora rode: sdtool verify --sys", o.Sys)
}

func verify(ctx context.Context, b Backend, file string, idle time.Duration) {
	acked := map[uint64]bool{}
	f, err := os.Open(file)
	check(err)
	sc := bufio.NewScanner(f)
	sc.Scan() // cabeçalho
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		if len(p) >= 2 && p[1] == "1" {
			if id, err := strconv.ParseUint(p[0], 10, 64); err == nil {
				acked[id] = true
			}
		}
	}
	f.Close()

	seen := map[uint64]int{}
	var maxID uint64
	total, ooo := 0, 0
	err = b.Consume(ctx, idle, func(p []byte, pos string) {
		var m Msg
		if json.Unmarshal(p, &m) != nil {
			return
		}
		total++
		if seen[m.ID] > 0 {
			seen[m.ID]++
			return
		}
		seen[m.ID] = 1
		if m.ID < maxID {
			ooo++
			fmt.Printf("fora de ordem: id=%d chegou após id=%d (%s)\n", m.ID, maxID, pos)
		} else {
			maxID = m.ID
		}
	})
	check(err)

	var lost []uint64
	for id := range acked {
		if seen[id] == 0 {
			lost = append(lost, id)
		}
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i] < lost[j] })
	dups, ghosts := 0, 0
	for id, c := range seen {
		dups += c - 1
		if !acked[id] {
			ghosts++
		}
	}
	fmt.Println("\n===== RESUMO DA VERIFICAÇÃO =====")
	fmt.Printf("lidas=%d  únicas=%d  confirmadas ao produtor=%d\n", total, len(seen), len(acked))
	fmt.Printf("PERDIDAS (confirmadas mas ausentes): %d %v\n", len(lost), lost)
	fmt.Printf("DUPLICADAS: %d\n", dups)
	fmt.Printf("FORA DE ORDEM: %d\n", ooo)
	fmt.Printf("FANTASMAS (erro no produtor, mas gravadas): %d\n", ghosts)
}

func bench(b Backend, o Opts, n, runs, size int) {
	var lats, tps []float64
	fmt.Printf("bench: sys=%s acks=%s idem=%v n=%d size=%dB\n", o.Sys, o.Acks, o.Idem, n, size)
	for r := 1; r <= runs; r++ {
		var sum float64
		okN := 0
		start := time.Now()
		for i := 0; i < n; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
			t0 := time.Now()
			_, err := b.Produce(ctx, uint64(r*1_000_000+i), mkPayload(uint64(i), size))
			cancel()
			if err == nil {
				okN++
				sum += ms(time.Since(t0))
			}
		}
		el := time.Since(start).Seconds()
		if okN == 0 {
			fmt.Printf("run %d: todas falharam\n", r)
			continue
		}
		l, t := sum/float64(okN), float64(okN)/el
		lats, tps = append(lats, l), append(tps, t)
		fmt.Printf("run %d: latência média=%.2f ms  vazão=%.0f msg/s  erros=%d\n", r, l, t, n-okN)
	}
	lm, ls := meanStd(lats)
	tm, ts := meanStd(tps)
	fmt.Printf("\nRESULTADO (%d execuções): latência = %.2f ± %.2f ms | vazão = %.0f ± %.0f msg/s\n", len(lats), lm, ls, tm, ts)
}
