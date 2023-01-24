package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastleader	int 	//the id of leader found by previous RPC
	mu					sync.Mutex
	clientId		int64	//the unique id for client
	seqNum 			int		//the unique id for command
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastleader = 0
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	seqNum :=ck.seqNum+1
	args:=GetArgs{
		Key: 			key,
		ClientId:	ck.clientId,
		SeqNum:		seqNum,
	}
	serverId := ck.lastleader
	for ; ; serverId=(serverId+1)%len(ck.servers){
		reply:=GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get",&args,&reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader{
			continue
		}
		ck.lastleader = serverId
		ck.seqNum = seqNum
		if reply.Err == ErrNoKey{
			return ""
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seqNum :=ck.seqNum+1
	args:=PutAppendArgs{
		Key: 			key,
		Value:		value,
		Op:				op,
		ClientId:	ck.clientId,
		SeqNum:		seqNum,
	}
	serverId := ck.lastleader
	for ; ; serverId=(serverId+1)%len(ck.servers){
		reply:=PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend",&args,&reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader{
			continue
		}
		ck.lastleader = serverId
		ck.seqNum = seqNum
		return 
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
