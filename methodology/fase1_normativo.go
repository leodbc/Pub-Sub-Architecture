// Targeted M2 normative demonstrations. No changes to the original sdtool.
package main
import (
 "context"
 "encoding/json"
 "flag"
 "fmt"
 "os"
 "sort"
 "time"
 "github.com/twmb/franz-go/pkg/kadm"
 "github.com/twmb/franz-go/pkg/kgo"
)
const seed = "172.31.0.41:19092"
func must(err error){if err!=nil{panic(err)}}
func client(opts ...kgo.Opt)*kgo.Client{base:=[]kgo.Opt{kgo.SeedBrokers(seed,"172.31.0.42:19092","172.31.0.43:19092")};c,e:=kgo.NewClient(append(base,opts...)...);must(e);return c}
func setup(t string,n int32){c:=client();defer c.Close();a:=kadm.NewClient(c);ctx,cancel:=context.WithTimeout(context.Background(),50*time.Second);defer cancel()
 d,e:=a.CreateTopic(ctx,n,3,nil,t);_ = d;must(e)
}
func produce(t,key,val string)(int32,int64){c:=client(kgo.ProducerLinger(0));defer c.Close();ctx,stop:=context.WithTimeout(context.Background(),15*time.Second);defer stop()
 r:=&kgo.Record{Topic:t,Key:[]byte(key),Value:[]byte(val)}
 must(c.ProduceSync(ctx,r).FirstErr());return r.Partition,r.Offset
}
func e2(t string){
 setup(t,3);expected:=map[string]struct{key string;part int32;offset int64}{}
 keys:=[]string{"cliente1","cliente2","cliente3","cliente4","cliente5","cliente6","cliente7","cliente8","cliente9","cliente10","cliente11","cliente12"}
 const each=3
 for round:=0;round<each;round++{for _,key:=range keys{
  id:=fmt.Sprintf("m2e2:%s:%02d",key,round)
  part,off:=produce(t,key,id)
  if prev,ok:=expected[fmt.Sprintf("m2e2:%s:%02d",key,round-1)];round>0&&ok&&prev.part!=part{panic("same key changed partitions")}
  expected[id]=struct{key string;part int32;offset int64}{key,part,off}
  fmt.Printf("PRODUCE key=%s partition=%d offset=%d id=%s\n",key,part,off,id)
 }}
 consumer:=client(kgo.ConsumeTopics(t),kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
 defer consumer.Close()
 seen:=map[string]bool{};parts:=map[int32]bool{};last:=map[string]int64{}
 ctx,cancel:=context.WithTimeout(context.Background(),70*time.Second);defer cancel()
 for len(seen)<len(expected) && ctx.Err()==nil{
  f:=consumer.PollFetches(ctx)
  f.EachRecord(func(r *kgo.Record){
   id:=string(r.Value);exp,ok:=expected[id];if !ok{return}
   if seen[id]{panic("duplicate read "+id)}
   if string(r.Key)!=exp.key||r.Partition!=exp.part||r.Offset!=exp.offset{panic("read mismatch "+id)}
   if previous,present:=last[exp.key];present&&r.Offset<=previous{panic("same-key offsets not increasing")}
   last[exp.key]=r.Offset;seen[id]=true;parts[r.Partition]=true
   fmt.Printf("CONSUME key=%s partition=%d offset=%d id=%s\n",r.Key,r.Partition,r.Offset,id)
  })
 }
 if len(seen)!=len(expected)||len(parts)<2{panic(fmt.Sprintf("E2 incomplete: read=%d want=%d partitions=%d",len(seen),len(expected),len(parts)))}
 fmt.Printf("E2_OK messages=%d keys=%d observed_partitions=%d same_key_stable=true read_offsets_match=true\n",len(seen),len(keys),len(parts))
}
func groupClient(topic,group string)*kgo.Client{
 return client(kgo.ConsumeTopics(topic),kgo.ConsumerGroup(group),kgo.DisableAutoCommit(),
  kgo.SessionTimeout(10*time.Second),kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
}
func groupSeed(topic string){setup(topic,1);for i:=0;i<6;i++{
 part,off:=produce(topic,"mesma-chave",fmt.Sprintf("group-%02d",i))
 fmt.Printf("SEEDED partition=%d offset=%d record=group-%02d\n",part,off,i)
}}
func groupFirst(topic,group string){
 c:=groupClient(topic,group)
 // Intentionally never Close: a process exiting without LeaveGroup simulates interruption.
 ctx,cancel:=context.WithTimeout(context.Background(),75*time.Second);defer cancel()
 var selected []*kgo.Record
 for len(selected)<2 && ctx.Err()==nil{
  f:=c.PollFetches(ctx)
  f.EachRecord(func(r *kgo.Record){if len(selected)<2{selected=append(selected,r)}})
 }
 if len(selected)<2{panic("first consumer could not read two records")}
 for _,r:=range selected{fmt.Printf("FIRST_CONSUME partition=%d offset=%d value=%s\n",r.Partition,r.Offset,r.Value)}
 if selected[0].Offset!=0||selected[1].Offset!=1{panic("new topic not starting from offset zero")}
 commitCtx,stop:=context.WithTimeout(ctx,20*time.Second);defer stop()
 must(c.CommitRecords(commitCtx,selected[1]))
 fmt.Printf("COMMIT_CONFIRMED partition=%d processed_offset=%d next_offset=%d group=%s\n",selected[1].Partition,selected[1].Offset,selected[1].Offset+1,group)
 os.Exit(0) // Abrupt stop: no Close/LeaveGroup and no automatic commit.
}
func groupResume(topic,group string){
 c:=groupClient(topic,group);defer c.Close()
 ctx,stop:=context.WithTimeout(context.Background(),115*time.Second);defer stop()
 seen:=map[int64]string{}
 for len(seen)<4 && ctx.Err()==nil{
  f:=c.PollFetches(ctx)
  f.EachRecord(func(r *kgo.Record){
   if _,exists:=seen[r.Offset];exists{return}
   seen[r.Offset]=string(r.Value)
   fmt.Printf("RESUMED partition=%d offset=%d value=%s\n",r.Partition,r.Offset,r.Value)
  })
 }
 if len(seen)!=4{panic(fmt.Sprintf("resumed %d records, expected 4",len(seen)))}
 offsets:=make([]int,0,4)
 for k:=range seen{offsets=append(offsets,int(k))}
 sort.Ints(offsets)
 for i,k:=range offsets{if k!=i+2||seen[int64(k)]!=fmt.Sprintf("group-%02d",k){panic("resume offset mismatch")}}
 fmt.Printf("RESUME_OK group=%s first_offset=%d last_offset=%d committed_previously=2 no_replay_of_committed=true\n",group,offsets[0],offsets[3])
}
func epochs(topic string){
 c:=client();defer c.Close();a:=kadm.NewClient(c)
 ctx,stop:=context.WithTimeout(context.Background(),20*time.Second);defer stop()
 details,err:=a.ListTopics(ctx,topic);must(err)
 rows:=[]map[string]int32{}
 for _,p:=range details[topic].Partitions.Sorted(){
  if p.Err!=nil {panic(p.Err)}
  if p.LeaderEpoch<0{panic("leader epoch not supported")}
  rows=append(rows,map[string]int32{"partition":p.Partition,"leader":p.Leader,"leader_epoch":p.LeaderEpoch})
 }
 if len(rows)==0{panic("no partition metadata")}
 b,err:=json.Marshal(rows);must(err);fmt.Printf("PARTITION_EPOCHS=%s\n",b)
}
func main(){
 mode:=flag.String("mode","","e2,group-seed,group-first,group-resume,epochs")
 topic:=flag.String("topic","m2_e2_keys","lab topic")
 group:=flag.String("group","m2-e7-group","consumer group")
 flag.Parse()
 switch *mode{
 case "e2":e2(*topic)
 case "group-seed":groupSeed(*topic)
 case "group-first":groupFirst(*topic,*group)
 case "group-resume":groupResume(*topic,*group)
 case "epochs":epochs(*topic)
 default:panic("invalid mode")
 }
}
