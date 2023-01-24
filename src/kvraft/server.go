package kvraft
import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"time"
	"log"
	"sync"
	"sync/atomic"
)
//---------------------------------Get/Put/Append-----from KVServer to Raft-----------------------------------
//The operation struct that will go into raft code(i.e. rf.start(op))
type Op struct {
	CommandType CommandType  // put,append,get
	Key  				string			 
	Value 			string			 // empty when the CommandType is Get
	ClientId		int64				 // which client perform this operation				
	SeqNum			int					 // The seqNum of this operation for the client
}
//The kv-server keep track of each client's latest operation's result
//To prevent duplicate operation
type OperationResult struct {
	SeqNum 		int            		
	Result   	ApplyNotifyMsg 		//该operation的响应(result)
}
// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,
	//但是后来没有成功commit就变成了follower,
	//导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader
	//的commit的log了
	Term int
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch,ok := kv.replyChMap[index]
	if !ok{
		return
	}
	close(ch)
	delete(kv.replyChMap,index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer func() {
		DPrintf("kvserver[%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	}()
	DPrintf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	if operationresult,ok := kv.clientReply[args.ClientId]; ok {
		//1.This request has already been executed,just return the result
		if operationresult.SeqNum == args.SeqNum{
			reply.Err = operationresult.Result.Err
			reply.Value = operationresult.Result.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.This request has not been executed,then generated an Op and pass it to raft
	op:=Op{
		CommandType: GetMethod,
		Key:				 args.Key,
		ClientId:		 args.ClientId,
		SeqNum:			 args.SeqNum,
	}
	index,term,isLeader:=kv.rf.Start(op)
	//3.Not leader,just return
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	//4.Wait for Raft finish
	replyCh := make(chan ApplyNotifyMsg,1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh   //we use index to uniquely identify this channel
	kv.mu.Unlock()
	select{
	case replyMsg := <-replyCh:
		DPrintf("kvserver[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", kv.me, index, replyMsg)
		if term == replyMsg.Term{
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrTimeout	
	}
	//5.clean channel
	go kv.CloseChan(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if operationresult,ok := kv.clientReply[args.ClientId]; ok {
		//1.This request has already been executed,just return the result
		if operationresult.SeqNum == args.SeqNum{
			reply.Err = operationresult.Result.Err
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.This request has not been executed,then generated an Op and pass it to raft
	op:=Op{
		CommandType: CommandType(args.Op),
		Key:				 args.Key,
		Value:			 args.Value,
		ClientId:		 args.ClientId,
		SeqNum:			 args.SeqNum,
	}
	index,term,isLeader:=kv.rf.Start(op)
	//3.Not leader,just return
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	//4.Wait for Raft finish
	replyCh := make(chan ApplyNotifyMsg,1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select{
	case replyMsg := <-replyCh:
		if term == replyMsg.Term{
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrTimeout	
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
	}
	
	//5.clean channel
	go kv.CloseChan(index)
	DPrintf("kvserver[%d]:PutAppend return",kv.me)
}
//-----------------------Apply Command/Snapshot---------------from Raft to KVServer-----------------------
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	proportion := float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion>0.9
}

func (kv *KVServer) startSnapshot(index int) {
	snapshot := kv.createSnapshot()
	go kv.rf.Snapshot(index,snapshot)
}

func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.kvDataBase)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.clientReply)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		select {
		 	case applyMsg := <-kv.applyCh:
				DPrintf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
				//当为合法命令时
				if applyMsg.CommandValid {
					 kv.ApplyCommand(applyMsg)
				} else if applyMsg.SnapshotValid {
					 //当为合法快照时
					 kv.ApplySnapshot(applyMsg)
				} else {
					 //非法消息
					 DPrintf("kvserver[%d]: error applyMsg from applyCh: %v\n", kv.me, applyMsg)
				}
		}
	}
}

func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var applynotifymsg ApplyNotifyMsg
	index, operation, term := applyMsg.CommandIndex, applyMsg.Command.(Op), applyMsg.CommandTerm
	if operationresult,ok := kv.clientReply[operation.ClientId]; ok {
		//1.This request has already been executed,do nothing
		if operationresult.SeqNum == operation.SeqNum{
			return
		}
	}
	//2.We need to excute this operation in KV database
	if operation.CommandType == GetMethod {
		if value,ok := kv.storeInterface.Get(operation.Key);ok{
			applynotifymsg = ApplyNotifyMsg{OK,value,term}
		}else{
			applynotifymsg = ApplyNotifyMsg{ErrNoKey,"",term}
		}
	}else if operation.CommandType == PutMethod {
		kv.storeInterface.Put(operation.Key,operation.Value)
		applynotifymsg = ApplyNotifyMsg{OK,"",term}
	}else{
		kv.storeInterface.Append(operation.Key,operation.Value)
		applynotifymsg = ApplyNotifyMsg{OK,"",term}
	}
	//3.We now notify that we have already applied
	if replych,ok := kv.replyChMap[index]; ok{
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.me, applyMsg, index)
		replych <- applynotifymsg
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.me, applyMsg, index)
	}
	//4.update client's latest operation result
	kv.clientReply[operation.ClientId] = OperationResult{
		SeqNum:    operation.SeqNum,
		Result:		 applynotifymsg,
	}
	kv.lastApplied = index
	//5.Need snapshot?
	if kv.needSnapshot() {
		kv.startSnapshot(index)
	}
}

func (kv *KVServer) ApplySnapshot(msg raft.ApplyMsg){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotIndex < kv.lastApplied{
		return
	}
	//raft layer snapshot
	kv.rf.Snapshot(msg.SnapshotIndex,msg.Snapshot)
	kv.lastApplied = msg.SnapshotIndex
	//KV layer snapshot
	kv.readSnapshot(msg.Snapshot)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvDataBase KvDataBase
	var clientReply map[int64]OperationResult
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil {
		DPrintf("kvserver[%d]: decode error\n", kv.me)
	} else {
		kv.kvDataBase = kvDataBase
		kv.clientReply = clientReply
		kv.storeInterface = &kvDataBase
	}
}
//--------------------------------------------APIs----------------------------------------------
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
//The in-memory KV database will provide this interface
type store interface{
	Get(key string) (value string,ok bool)
	Put(key string, value string) (newValue string)
	Append(key string, arg string) (newValue string)
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	
	// Your definitions here.
	clientReply		 map[int64]OperationResult 		
	kvDataBase     KvDataBase                  //数据库,可自行定义和更换
	storeInterface store                       //数据库接口
	replyChMap     map[int]chan ApplyNotifyMsg //某index的响应的chan
	lastApplied    int                         //上一条应用的log的index,防止快照导致回退
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.clientReply = make(map[int64]OperationResult)
	kv.kvDataBase = KvDataBase{make(map[string]string)}
	kv.storeInterface = &kv.kvDataBase
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.ReceiveApplyMsg()
	return kv
}
