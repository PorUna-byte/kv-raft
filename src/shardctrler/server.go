package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "sync/atomic"
import "sort"
//------------------------Join/Leave/Move/Query--------------from ShardCtrler to raft-----------------------------------------
type Op struct {
	// Your data here.
	CommandType CommandType  			//Join,Leave,Move,Query
	Servers 		map[int][]string 	//Join 	only
	GIDs 				[]int							//Leave only
	Shard		 		int								//Move 	only
	GID   			int								//Move 	only
	Num 				int 							//Query only
	ClientId    int64
	SeqNum      int
}
//The ShardCtrler keep track of each client's latest operation's result
//To prevent duplicate operation
type OperationResult struct {
	SeqNum 		int            		
	Result   	ApplyNotifyMsg 		//该operation的响应(result)
} 
// ApplyNotifyMsg 可表示JoinReply,LeaveReply,MoveReply,QueryReply
type ApplyNotifyMsg struct {
	WrongLeader bool
	Err         Err
	Config      Config   //Only for QueryReply
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,
	//但是后来没有成功commit就变成了follower,
	//导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader
	//的commit的log了
	Term int
}
func (sc *ShardCtrler) CloseChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch,ok := sc.replyChMap[index]
	if !ok{
		return
	}
	close(ch)
	delete(sc.replyChMap,index)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer func() {
		DPrintf("ShardCtrler[%d]: 返回Join RPC请求,args=[%v];Reply=[%v]\n", sc.me, args, reply)
	}()
	DPrintf("ShardCtrler[%d]: 接收Join RPC请求,args=[%v]\n", sc.me, args)
	if operationresult,ok := sc.clientReply[args.ClientId]; ok {
		//1.This request has already been executed,just return the result
		if operationresult.SeqNum == args.SeqNum{
			reply.Err = operationresult.Result.Err
			reply.WrongLeader = operationresult.Result.WrongLeader
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	//2.This request has not been executed,then generated an Op and pass it to raft
	op:=Op{
		CommandType: JoinMethod,
		Servers:		 args.Servers,
		ClientId:		 args.ClientId,
		SeqNum:			 args.SeqNum,
	}
	index,term,isLeader:=sc.rf.Start(op)
	//3.Not leader,just return
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	//4.Wait for Raft finish
	replyCh := make(chan ApplyNotifyMsg,1)
	sc.mu.Lock()
	sc.replyChMap[index] = replyCh   //we use index to uniquely identify this channel
	sc.mu.Unlock()
	select{
	case replyMsg := <-replyCh:
		DPrintf("ShardCtrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
		if term == replyMsg.Term{
			reply.Err = replyMsg.Err
			reply.WrongLeader = replyMsg.WrongLeader
		} else {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = ErrTimeout	
	}
	//5.clean channel
	go sc.CloseChan(index)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
		// Your code here.
		sc.mu.Lock()
		defer func() {
			DPrintf("ShardCtrler[%d]: 返回Leave RPC请求,args=[%v];Reply=[%v]\n", sc.me, args, reply)
		}()
		DPrintf("ShardCtrler[%d]: 接收Leave RPC请求,args=[%v]\n", sc.me, args)
		if operationresult,ok := sc.clientReply[args.ClientId]; ok {
			//1.This request has already been executed,just return the result
			if operationresult.SeqNum == args.SeqNum{
				reply.Err = operationresult.Result.Err
				reply.WrongLeader = operationresult.Result.WrongLeader
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()
		//2.This request has not been executed,then generated an Op and pass it to raft
		op:=Op{
			CommandType: LeaveMethod,
			GIDs:				 args.GIDs,
			ClientId:		 args.ClientId,
			SeqNum:			 args.SeqNum,
		}
		index,term,isLeader:=sc.rf.Start(op)
		//3.Not leader,just return
		if !isLeader{
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		}
		//4.Wait for Raft finish
		replyCh := make(chan ApplyNotifyMsg,1)
		sc.mu.Lock()
		sc.replyChMap[index] = replyCh   //we use index to uniquely identify this channel
		sc.mu.Unlock()
		select{
		case replyMsg := <-replyCh:
			DPrintf("ShardCtrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
			if term == replyMsg.Term{
				reply.Err = replyMsg.Err
				reply.WrongLeader = replyMsg.WrongLeader
			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			}
		case <-time.After(500*time.Millisecond):
			reply.Err = ErrTimeout	
		}
		//5.clean channel
		go sc.CloseChan(index)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
			// Your code here.
			sc.mu.Lock()
			defer func() {
				DPrintf("ShardCtrler[%d]: 返回Move RPC请求,args=[%v];Reply=[%v]\n", sc.me, args, reply)
			}()
			DPrintf("ShardCtrler[%d]: 接收Move RPC请求,args=[%v]\n", sc.me, args)
			if operationresult,ok := sc.clientReply[args.ClientId]; ok {
				//1.This request has already been executed,just return the result
				if operationresult.SeqNum == args.SeqNum{
					reply.Err = operationresult.Result.Err
					reply.WrongLeader = operationresult.Result.WrongLeader
					sc.mu.Unlock()
					return
				}
			}
			sc.mu.Unlock()
			//2.This request has not been executed,then generated an Op and pass it to raft
			op:=Op{
				CommandType: MoveMethod,
				Shard:			 args.Shard,
				GID:				 args.GID,
				ClientId:		 args.ClientId,
				SeqNum:			 args.SeqNum,
			}
			index,term,isLeader:=sc.rf.Start(op)
			//3.Not leader,just return
			if !isLeader{
				reply.WrongLeader = true
				reply.Err = ErrWrongLeader
				return
			}
			//4.Wait for Raft finish
			replyCh := make(chan ApplyNotifyMsg,1)
			sc.mu.Lock()
			sc.replyChMap[index] = replyCh   //we use index to uniquely identify this channel
			sc.mu.Unlock()
			select{
			case replyMsg := <-replyCh:
				DPrintf("ShardCtrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
				if term == replyMsg.Term{
					reply.Err = replyMsg.Err
					reply.WrongLeader = replyMsg.WrongLeader
				} else {
					reply.Err = ErrWrongLeader
					reply.WrongLeader = true
				}
			case <-time.After(500*time.Millisecond):
				reply.Err = ErrTimeout	
			}
			//5.clean channel
			go sc.CloseChan(index)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
			// Your code here.
			sc.mu.Lock()
			defer func() {
				DPrintf("ShardCtrler[%d]: 返回Query RPC请求,args=[%v];Reply=[%v]\n", sc.me, args, reply)
			}()
			DPrintf("ShardCtrler[%d]: 接收Query RPC请求,args=[%v]\n", sc.me, args)
			if operationresult,ok := sc.clientReply[args.ClientId]; ok {
				//1.This request has already been executed,just return the result
				if operationresult.SeqNum == args.SeqNum{
					reply.Err = operationresult.Result.Err
					reply.WrongLeader = operationresult.Result.WrongLeader
					reply.Config = operationresult.Result.Config
					sc.mu.Unlock()
					return
				}
			}
			sc.mu.Unlock()
			//2.This request has not been executed,then generated an Op and pass it to raft
			op:=Op{
				CommandType: QueryMethod,
				Num:				 args.Num,
				ClientId:		 args.ClientId,
				SeqNum:			 args.SeqNum,
			}
			index,term,isLeader:=sc.rf.Start(op)
			//3.Not leader,just return
			if !isLeader{
				reply.WrongLeader = true
				reply.Err = ErrWrongLeader
				return
			}
			//4.Wait for Raft finish
			replyCh := make(chan ApplyNotifyMsg,1)
			sc.mu.Lock()
			sc.replyChMap[index] = replyCh   //we use index to uniquely identify this channel
			sc.mu.Unlock()
			select{
			case replyMsg := <-replyCh:
				DPrintf("ShardCtrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
				if term == replyMsg.Term{
					reply.Err = replyMsg.Err
					reply.WrongLeader = replyMsg.WrongLeader
					reply.Config = replyMsg.Config
				} else {
					reply.Err = ErrWrongLeader
					reply.WrongLeader = true
				}
			case <-time.After(500*time.Millisecond):
				reply.Err = ErrTimeout	
			}
			//5.clean channel
			go sc.CloseChan(index)
}



//------------------------------------Apply Command------------------from raft to ShardCtrler--------------------------------
func (sc *ShardCtrler) ReceiveApplyMsg() {
	//The shardctrler will always live, it won't be killed.
	for !sc.killed(){
		select {
		 	case applyMsg := <-sc.applyCh:
				DPrintf("ShardCtrler[%d]: 获取到applyCh中新的applyMsg=[%v]\n", sc.me, applyMsg)
				//当为合法命令时
				if applyMsg.CommandValid {
					sc.ApplyCommand(applyMsg)
				} else {
					 //非法消息,这里不考虑snapshot的情况,因为ShardCtrler未使用到snapshot
					 DPrintf("ShardCtrler[%d]: error applyMsg from applyCh: %v\n", sc.me, applyMsg)
				}
		}
	}
}
func (sc *ShardCtrler) ApplyCommand(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var applynotifymsg ApplyNotifyMsg
	index, operation, term := applyMsg.CommandIndex, applyMsg.Command.(Op), applyMsg.CommandTerm
	if operationresult,ok := sc.clientReply[operation.ClientId]; ok {
		//1.This request has already been executed,do nothing
		if operationresult.SeqNum == operation.SeqNum{
			return
		}
	}
	//2.We need to excute this operation
	if operation.CommandType == JoinMethod {
		num := sc.confignum+1
		old_config := sc.configs[sc.confignum]
		var new_config Config
		new_config.Num = num
		new_config.Groups = map[int][]string{}		
		new_gidset := make([]int,0)  //To keep track of new gids
		// total_gidset := make([]int,0)   //To keep track of total gids
		for gid := range old_config.Groups{
			new_config.Groups[gid]=old_config.Groups[gid]
			// total_gidset = append(total_gidset,gid)
		}
		for gid := range operation.Servers{
			if _,ok := new_config.Groups[gid]; ok{
				DPrintf("ShardCtrler[%d]:Duplicate gid for JoinMethod", sc.me)
				applynotifymsg = ApplyNotifyMsg{
					WrongLeader:  false,
					Err:					ErrJoinDup,
					Term:					term,
				}
				goto flag
			}
			new_config.Groups[gid]=operation.Servers[gid]
			// total_gidset = append(total_gidset,gid)
			new_gidset = append(new_gidset,gid)
		}
		threshold := NShards/len(new_config.Groups)  //we divide the shards as evenly as possible
		counts := map[int]int{}   //A map from gid to the number of shards it supports
		free_set := make([]int,0)
		for i:=0;i<NShards;i++ {
			gid := old_config.Shards[i]
			if _,ok := counts[gid];ok{
				counts[gid]++
			}else{
				counts[gid]=1
			}
			if counts[gid]>threshold || gid==0{
				free_set = append(free_set,i)
			}
		}
		sort.Ints(free_set)
		sort.Ints(new_gidset)
		new_config.Shards = old_config.Shards
		for idx,value := range free_set{
			new_config.Shards[value] = new_gidset[idx%len(new_gidset)]
		}
		// sort.Ints(total_gidset)
		// for i:=0;i<NShards;i++{
		// 	new_config.Shards[i]=total_gidset[i%len(total_gidset)]
		// }
		sc.confignum = num
		sc.configs = append(sc.configs,new_config)
		applynotifymsg = ApplyNotifyMsg{
			WrongLeader:  false,
			Err:					OK,
			Term:					term,
		}
	}else if operation.CommandType == LeaveMethod {
		num := sc.confignum+1
		old_config := sc.configs[sc.confignum]
		var new_config Config
		new_config.Num = num
		new_config.Groups = map[int][]string{}
		free_set := make([]int,0)  		//free shard number due to the leave of some groups
		// remain_gids := make([]int,0)  //remain gids due to the leave of some groups
		total_gidset := make([]int,0)
		for gid := range old_config.Groups{
			if !isInset(operation.GIDs,gid){
				new_config.Groups[gid]=old_config.Groups[gid]
				// remain_gids = append(remain_gids,gid)
				total_gidset = append(total_gidset,gid)
			}
		}
		// var threshold int
		// if len(new_config.Groups)!=0{
		// 	threshold = NShards/len(remain_gids)  //we divide the shards as evenly as possible
		// }
		// counts := map[int]int{}   //A map from gid to the number of shards it supports
		for i:=0;i<NShards;i++{
			gid := old_config.Shards[i]	
			if isInset(operation.GIDs,gid){
				free_set = append(free_set,i)
			}else{
				new_config.Shards[i]=gid
				// if _,ok:=counts[gid];ok{
				// 	counts[gid]++
				// }else{
				// 	counts[gid]=1
				// }
			}
		}
		// reassign free_set to remain_gids
		// sort.Ints(free_set)
		// sort.Ints(remain_gids)
		// i:=0
		// for idx:=0;idx<len(free_set);i++{
		// 	value := free_set[idx]
		// 	if len(remain_gids)!=0{
		// 		gid := remain_gids[i%len(remain_gids)]
		// 		if counts[gid]<=threshold+1{
		// 			new_config.Shards[value]= gid
		// 			counts[gid]++
		// 			idx++
		// 		}
		// 	}else{
		// 		idx++
		// 		new_config.Shards[value]= 0   //No remain groups, pseudo(invalid) gid=0
		// 	}
		// }
		sort.Ints(total_gidset)
		for i:=0;i<NShards;i++{
			if(len(total_gidset)!=0){
				new_config.Shards[i]=total_gidset[i%len(total_gidset)]
			}else{
				new_config.Shards[i]=0
			}
		}
		sc.confignum = num
		sc.configs = append(sc.configs,new_config)
		applynotifymsg = ApplyNotifyMsg{
			WrongLeader:  false,
			Err:					OK,
			Term:					term,
		}
	}else if operation.CommandType == MoveMethod{
		num := sc.confignum+1
		old_config := sc.configs[sc.confignum]
		var new_config Config
		new_config.Num = num
		new_config.Groups = map[int][]string{}
		for gid := range old_config.Groups{
			new_config.Groups[gid]=old_config.Groups[gid]
		}
		new_config.Shards = old_config.Shards
		new_config.Shards[operation.Shard]=operation.GID
		sc.confignum = num
		sc.configs = append(sc.configs,new_config)
		applynotifymsg = ApplyNotifyMsg{
			WrongLeader:  false,
			Err:					OK,
			Term:					term,
		}
	}else{
		//Here is QueryMethod
		var confignum int
		if operation.Num ==-1 || operation.Num>sc.confignum{
			confignum=sc.confignum
		}else{
			confignum=operation.Num
		}

		applynotifymsg = ApplyNotifyMsg{
			WrongLeader:  false,
			Err:					OK,
			Config:				sc.configs[confignum],
			Term:					term,
		}
	}

	flag:
	//3.We now notify that we have already applied
	if replych,ok := sc.replyChMap[index]; ok{
		DPrintf("ShardCtrler[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", sc.me, applyMsg, index)
		replych <- applynotifymsg
		DPrintf("ShardCtrler[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", sc.me, applyMsg, index)
	}

	//4.update client's latest operation result
	sc.clientReply[operation.ClientId] = OperationResult{
		SeqNum:    operation.SeqNum,
		Result:		 applynotifymsg,
	}
}


//-----------------------------------------------------APIs--------------------------------------------------------------------
//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}
// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

type ShardCtrler struct {
	mu     			sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	configs 		[]Config 										// indexed by config num
	// Your data here.
	confignum 	int													//number of total configurations
	replyChMap  map[int]chan ApplyNotifyMsg //某index的响应的chan
	clientReply	map[int64]OperationResult 	
	dead				int32
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.confignum=0
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.replyChMap = make(map[int]chan ApplyNotifyMsg)
	sc.clientReply = make(map[int64]OperationResult)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.ReceiveApplyMsg()
	return sc
}
