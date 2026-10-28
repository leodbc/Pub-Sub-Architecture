// visibility.go: measured client-visible delivery (NOT follower propagation).
// A consumer starts before the timed writes; a warm-up record confirms readiness.
package main

import (
 "context"
 "encoding/csv"
 "encoding/json"
 "flag"
 "fmt"
 "os"
 "strconv"
 "strings"
 "time"

 "github.com/nats-io/nats.go"
 "github.com/nats-io/nats.go/jetstream"
 "github.com/twmb/franz-go/pkg/kgo"
)

type message struct { ID uint64 `json:"id"` }
type observed struct { ID uint64; At time.Time }
type producer func(context.Context, []byte) error

func main() {
 system:=flag.String("sys","kafka","kafka or nats")
 acks:=flag.String("acks","all","Kafka: all or 1")
 runs:=flag.Int("runs",5,"number of observations")
 output:=flag.String("out","/tmp/visibility.csv","CSV path")
 flag.Parse()
 if *runs != 5 { panic("R7: runs must be exactly 5") }
 ctx,cancel:=context.WithCancel(context.Background())
 defer cancel()
 seen:=make(chan observed,2048)
 var publish producer
 var cleanup func()
 var err error
 switch *system {
 case "kafka":
  seeds:=[]string{"172.31.0.41:19092","172.31.0.42:19092","172.31.0.43:19092"}
  opts:=[]kgo.Opt{kgo.SeedBrokers(seeds...),kgo.ProducerLinger(0),kgo.DisableIdempotentWrite()}
  if *acks=="1" { opts=append(opts,kgo.RequiredAcks(kgo.LeaderAck())) } else if *acks=="all" {
   opts=append(opts,kgo.RequiredAcks(kgo.AllISRAcks()))
  } else { panic("invalid Kafka ack mode") }
  p,e:=kgo.NewClient(opts...); if e!=nil { panic(e) }
  consumer,e:=kgo.NewClient(kgo.SeedBrokers(seeds...),kgo.ConsumeTopics("probe"),
   kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
  if e!=nil { p.Close(); panic(e) }
  cleanup=func(){ consumer.Close();p.Close() }
  publish=func(ctx context.Context,b []byte) error {
   rec:=&kgo.Record{Topic:"probe",Key:[]byte("visibility"),Value:b}
   return p.ProduceSync(ctx,rec).FirstErr()
  }
  go func(){
   for ctx.Err()==nil {
    fetchCtx,stop:=context.WithTimeout(ctx,2*time.Second)
    fetched:=consumer.PollFetches(fetchCtx)
    stop()
    fetched.EachRecord(func(rec *kgo.Record){
     var m message
     if json.Unmarshal(rec.Value,&m)==nil {
      select { case seen<-observed{m.ID,time.Now()}: default: }
     }
    })
   }
  }()
 case "nats":
  nc,e:=nats.Connect("nats://172.33.0.51:4222,nats://172.33.0.52:4222,nats://172.33.0.53:4222",
   nats.MaxReconnects(2))
  if e!=nil { panic(e) }
  js,e:=jetstream.New(nc); if e!=nil { nc.Close();panic(e) }
  cons,e:=js.OrderedConsumer(ctx,"PROBE",jetstream.OrderedConsumerConfig{DeliverPolicy:jetstream.DeliverAllPolicy})
  if e!=nil {nc.Close();panic(e)}
  cc,e:=cons.Consume(func(m jetstream.Msg){
   var body message
   if json.Unmarshal(m.Data(),&body)==nil {
    select {case seen<-observed{body.ID,time.Now()}:default:}
   }
  })
  if e!=nil { nc.Close();panic(e) }
  cleanup=func(){cc.Stop();nc.Close()}
  publish=func(c context.Context,b []byte) error {_,e:=js.Publish(c,"probe.k",b);return e}
 default: panic("invalid system")
 }
 defer cleanup()
 file,err:=os.Create(*output);if err!=nil{panic(err)}
 defer file.Close()
 w:=csv.NewWriter(file)
 defer w.Flush()
 if err=w.Write([]string{"system","acks","run","ack_ms","read_visibility_ms","read_minus_ack_ms","producer_status","read_status","semantics"});err!=nil{panic(err)}
 base:=uint64(time.Now().UnixNano())
 send:=func(id uint64)(time.Time,time.Time,error){
  payload,_:=json.Marshal(message{ID:id})
  started:=time.Now()
  requestCtx,stop:=context.WithTimeout(ctx,9*time.Second)
  e:=publish(requestCtx,payload)
  stop()
  acked:=time.Now()
  if e!=nil{return started,acked,e}
  timer:=time.NewTimer(12*time.Second)
  defer timer.Stop()
  for {
   select {
   case obs:= <-seen:
    if obs.ID==id {return started,obs.At,nil}
   case <-timer.C:
    return started,acked,fmt.Errorf("read observation missing within 12s for %d",id)
   case <-ctx.Done():return started,acked,ctx.Err()
   }
  }
 }
 // The warmup is not a measured repetition.
 if _,_,e:=send(base); e!=nil{panic(fmt.Errorf("consumer-readiness warmup failed: %w",e))}
 for i:=1;i<=*runs;i++ {
  id:=base+uint64(i)
  payload,_:=json.Marshal(message{ID:id})
  started:=time.Now()
  deadline,stop:=context.WithTimeout(ctx,9*time.Second)
  e:=publish(deadline,payload)
  stop()
  acked:=time.Now()
  if e!=nil {
   _=w.Write([]string{*system,*acks,strconv.Itoa(i),"","","","error: "+e.Error(),"not_observed","client observation only"})
   w.Flush();file.Sync();panic(e)
  }
  var readAt time.Time
  timer:=time.NewTimer(12*time.Second)
 waitLoop:
  for {
   select {
   case obs:= <-seen:
    if obs.ID==id {readAt=obs.At;break waitLoop}
   case <-timer.C:
    break waitLoop
   }
  }
  timer.Stop()
  if readAt.IsZero() {
   _=w.Write([]string{*system,*acks,strconv.Itoa(i),fmt.Sprintf("%.3f",float64(acked.Sub(started).Microseconds())/1000),"","","acknowledged","timeout","client observation only"})
   w.Flush();file.Sync()
   panic(fmt.Errorf("record %d not observed",id))
  }
  ackMs:=float64(acked.Sub(started).Microseconds())/1000
  visMs:=float64(readAt.Sub(started).Microseconds())/1000
  diffMs:=float64(readAt.Sub(acked).Microseconds())/1000
  row:=[]string{*system,*acks,strconv.Itoa(i),fmt.Sprintf("%.3f",ackMs),fmt.Sprintf("%.3f",visMs),fmt.Sprintf("%.3f",diffMs),"acknowledged","observed","leader/stream consumer callback, NOT follower replication"}
  if e=w.Write(row);e!=nil{panic(e)}
  w.Flush();file.Sync()
  fmt.Println(strings.Join(row,","))
 }
 fmt.Printf("PASS: %d real client-visible samples, separate ACK and callback measurements\n",*runs)
}
