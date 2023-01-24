package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu					sync.Mutex
	clientId		int64	//the unique id for client
	seqNum 			int		//the unique id for command,which is combined with clientId to detect duplicate request
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
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	seqNum := ck.seqNum+1
	args := &QueryArgs{
		Num:  		num,
		ClientId: ck.clientId,
		SeqNum:		seqNum,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqNum = seqNum  //update ck.seqNum for next request
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	seqNum := ck.seqNum+1
	args := &JoinArgs{
		Servers:		servers,
		ClientId:		ck.clientId,
		SeqNum:			seqNum,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqNum = seqNum
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	seqNum := ck.seqNum+1
	args := &LeaveArgs{
		GIDs: 		gids,
		ClientId:	ck.clientId,
		SeqNum:		seqNum,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqNum = seqNum
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	seqNum := ck.seqNum+1
	args := &MoveArgs{
		Shard: 		shard,
		GID:   		gid,
		ClientId:	ck.clientId,
		SeqNum:		seqNum,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqNum = seqNum
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
