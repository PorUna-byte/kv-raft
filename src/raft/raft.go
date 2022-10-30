//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
package raft
import (
	"bytes"
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	"6.824/labgob"
	"6.824/labrpc"
)

//----------------------------------------Raft instance -------------------------------------------------//

type logEntry struct{	
	Command  		interface{} //command for state machine
	Term        int					//The term that the leader create this log entry
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh 	chan ApplyMsg	
	cond 			*sync.Cond
	heartbeat int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state
	CurrentTerm int 				//latest term server has seen
	VotedFor 		int 				//The candidate that you vote for in current term
	Log 				[]logEntry 	//log entries
	//Volatile state
	commitIndex 		int					//highest index of committed log entries
	lastApplied 		int         //highest index of applied(to state machine) log entries
	role						string 			//What's the server's role? leader,candidate,follower	
	elhe_timer			time.Time
	electionTimeout	int

	//leader only
	nextIndex   []int       //index of the next log entry to send to each server
	matchIndex  []int 			//highest known replicated log entry for each server 
}
func (rf *Raft) Convert2follower(term int){
	if rf.role!=follower_{
		Debug(dTerm,"S%d convert to follower",rf.me)
	}	
	rf.role=follower_
	if term>rf.CurrentTerm{
		rf.CurrentTerm = term
		//we must reset VotedFor if we update the CurrentTerm
		rf.VotedFor = -1
		rf.persist()
	}
	rf.electionTimeout = NewElection_timeout()
	rf.elhe_timer = time.Now()
}
func myTimer(mu *sync.Mutex, cond *sync.Cond, duration time.Duration) {
	go func() {
			time.Sleep(duration)
			mu.Lock()
			cond.Broadcast()
			mu.Unlock()
	}()
}
//-------------------------------------------leader election  -------------------------------------------------------
type RequestVoteArgs struct {
	Term    			int  //candidate's term
	CandidateId 	int  //candidate's Id
	LastLogIndex	int  //index of candidate’s last log entry
	LastLogTerm   int  //term of candidate’s last log entry
}
type RequestVoteReply struct {
	Term   			int 	//currentTerm, for candidate to update itself
	VoteGranted	bool	//Wheather vote for this candidate
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	loop := 0
	ok:=false
	for ;loop<30;loop++ {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok{
			break
		}
		time.Sleep(time.Millisecond*1)
	}
	return ok
}
func NewElection_timeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int()%350+300//timeout
}
func (rf *Raft) election() {
	for rf.killed() == false {
		time.Sleep(3*time.Millisecond)
		rf.mu.Lock()
		var Istimeout bool
		Istimeout = (time.Since(rf.elhe_timer)/time.Millisecond > time.Duration(rf.electionTimeout))
		if Istimeout{
			rf.elhe_timer = time.Now()
			rf.electionTimeout = NewElection_timeout()
		}
		if rf.role==follower_&&Istimeout{
			rf.role = candidate_
		}
		if rf.role==candidate_&&Istimeout{
		  rf.CurrentTerm ++
			// Debug(dTimer,"S%d start election for term%d",rf.me,rf.CurrentTerm)
			rf.VotedFor = rf.me
			rf.persist()
			args := RequestVoteArgs {rf.CurrentTerm,rf.me,len(rf.Log)-1,rf.Log[len(rf.Log)-1].Term}
			votecount := 1 
			finishcount := 1 
			cond := sync.NewCond(&rf.mu)
			checkpoint := time.Now()
			myTimer(&rf.mu,cond,time.Millisecond*time.Duration(42))
			//Send RequestVote RPCs to all other servers
			for server:=0;server<len(rf.peers)&&rf.role==candidate_;server++{
				rf.mu.Unlock()
				if server != rf.me{
					go func(server int,args RequestVoteArgs){
						rf.mu.Lock()
						if rf.role!=candidate_{
							finishcount++
							rf.mu.Unlock()
							cond.Broadcast()
							return
						}
						rf.mu.Unlock()
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(server, &args, &reply)
						if ok {
							rf.mu.Lock()
							if reply.Term > rf.CurrentTerm{
								rf.Convert2follower(reply.Term)
							}
							//old reply
							if rf.role != candidate_ || args.Term != rf.CurrentTerm {
								finishcount++
								rf.mu.Unlock()
								cond.Broadcast()
								return
							}			
							if reply.VoteGranted&&rf.CurrentTerm==args.Term{
								votecount++;
							}
							rf.mu.Unlock()
						}
						rf.mu.Lock()
						finishcount++
						cond.Broadcast()
						rf.mu.Unlock()
					}(server,args)
				}	
				rf.mu.Lock()
			}
			for finishcount!=len(rf.peers) && rf.role==candidate_&&votecount<=len(rf.peers)/2&&time.Since(checkpoint)/time.Millisecond<time.Duration(40){
				cond.Wait()
			}
			if votecount>len(rf.peers)/2 && rf.role==candidate_{
				rf.role = leader_
				Debug(dLeader,"S%d is chosen to be a new leader",rf.me)
				rf.nextIndex = make([]int,len(rf.peers))
				rf.matchIndex = make([]int,len(rf.peers))
				for server:=0;server<len(rf.peers);server++{
					rf.nextIndex[server] = len(rf.Log)
				}
				rf.matchIndex[rf.me] = len(rf.Log)-1
				rf.mu.Unlock()
				rf.append_entries2all()
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft)IsUp2date(args *RequestVoteArgs) bool{
	MyLastLogTerm := rf.Log[len(rf.Log)-1].Term
	if MyLastLogTerm<args.LastLogTerm{
		return true
	}else if MyLastLogTerm==args.LastLogTerm&&len(rf.Log)<=args.LastLogIndex+1{
		return true
	}else{
		return false
	}
}

// RequestVote RPC handler.
// The receiver part of leader election
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>rf.CurrentTerm{
		rf.Convert2follower(args.Term)
	}
	if args.Term<rf.CurrentTerm ||
	(rf.VotedFor!=-1&&rf.VotedFor!=args.CandidateId){
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return 
	}
	//compare the candidate and this server
	//see who is more up-to-date
	if rf.IsUp2date(args){
		rf.VotedFor=args.CandidateId
		reply.VoteGranted = true
		Debug(dVote,"S%d vote for S%d",rf.me,args.CandidateId)
		reply.Term = rf.CurrentTerm
		rf.elhe_timer = time.Now()
		rf.electionTimeout = NewElection_timeout()
		rf.persist()
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
	return
}

//--------------------------leader append entries to other followers & check commit---------------------------------------
type AppendEntriesArgs struct{
	Term 		 			int 					//leader's current term
	LeaderId 			int						//so follower can redirect clients
	PrevLogIndex 	int 					//index of log entry immediately preceding new ones
	PrevLogTerm   int 					//term of prevLogIndex entry
	Entries				[]logEntry		//log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit  int 					//leader's commidIndex
}	

type AppendEntriesReply struct{
	Term     			int  	//currentTerm of receiver,for leader to update itself
	Success  			bool 	//true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int		//The conflict index of the follower
	ConflictTerm  int   //The term of entry at conflict index
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft)check_majority(N int)bool{
	count:=0
	for server:=0;server<len(rf.peers);server++{
		if rf.matchIndex[server]>=N||server==rf.me { 
			//must check the case:server==rf.me
			count ++
		}
	}
	if count>len(rf.peers)/2{
		return true
	}else{
		return false
	}
}
func (rf *Raft)check_commit(){
	if rf.role==leader_{
		// Debug(dInfo,"leaderS%d check commit startwith commitIndex%d",rf.me,rf.commitIndex)
		for N:=len(rf.Log)-1;N>=rf.commitIndex+1;N--{
			if rf.Log[N].Term==rf.CurrentTerm&&rf.check_majority(N){
				Debug(dCommit,"leaderS%d commit log%d",rf.me,N)
				rf.commitIndex = N
				break
			}
		}
		rf.cond.Broadcast()
	}
}
func (rf *Raft)append_entries2all(){
	rf.mu.Lock()
	success_count,finish_count := 1,1
	cond := sync.NewCond(&rf.mu)
	checkpoint := time.Now()
	myTimer(&rf.mu,cond,time.Millisecond*time.Duration(42))
	for server:=0;server<len(rf.peers)&&rf.role==leader_;server++{
		rf.mu.Unlock()
		if server!=rf.me {
			go func(server int){
				loop := 0
				for ;loop<30;loop++ {	
					rf.mu.Lock()		
					if(rf.role!=leader_){
						finish_count ++
						cond.Broadcast()
						rf.mu.Unlock()
						return
					}
					args := AppendEntriesArgs{
						rf.CurrentTerm,
						rf.me,
						rf.nextIndex[server]-1,	
						rf.Log[rf.nextIndex[server]-1].Term,
						nil,
						rf.commitIndex}			
					if len(rf.Log)-1 >= rf.nextIndex[server]{
						//Try to append entries if nextIndex is shorter 
						args.Entries = rf.Log[rf.nextIndex[server]:]
					}
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					Debug(dLeader,"S%d send %dentries to S%d,the nextIndex is %d",rf.me,len(args.Entries),server,rf.nextIndex[server])
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						//if the reply is old reply
						rf.mu.Lock()
						if args.Term != rf.CurrentTerm || rf.role!=leader_{
							finish_count++
							cond.Broadcast()
							rf.mu.Unlock()
							return 
						}
						Debug(dInfo,"The append reply from %d is %v",server,reply.Success)
						if reply.Success {
							success_count++
							rf.matchIndex[server]	= Max(args.PrevLogIndex + len(args.Entries),rf.matchIndex[server])
							rf.nextIndex[server] = rf.matchIndex[server] + 1
							rf.mu.Unlock()
							break
						}else if reply.Term>rf.CurrentTerm{
							rf.Convert2follower(reply.Term)
						}else{
							//log inconsistency
							if(reply.ConflictTerm!=-1){
								idx := reply.ConflictIndex
								for ;idx>0&&rf.Log[idx].Term!=reply.ConflictTerm;idx--{}
								if(idx!=0){
									//found that term
									rf.nextIndex[server]=idx+1
								}else{
									rf.nextIndex[server]=reply.ConflictIndex
								}	
							}else{
								rf.nextIndex[server]=reply.ConflictIndex
							}
							rf.nextIndex[server]=Min(reply.ConflictIndex,rf.nextIndex[server])  //simplify our life
							Debug(dWarn,"nextIndex of S%d decrease to %d",server,rf.nextIndex[server])
						}
						rf.mu.Unlock()
					}
				}
				rf.mu.Lock()
				finish_count++
				cond.Broadcast()
				rf.mu.Unlock()
			}(server)
		}
		rf.mu.Lock()
	}
	for success_count<= len(rf.peers)/2 && finish_count!=len(rf.peers)&&rf.role==leader_&&time.Since(checkpoint)/time.Millisecond<time.Duration(40){
		cond.Wait()
	}
	rf.mu.Unlock()
}
//The sender part of leader append
func (rf *Raft)leader_announce(){
	//check for each follower wheather there are some entries to append
	//and update leader's commitIndex
	for rf.killed()==false{
		rf.mu.Lock()
		rf.check_commit()
		rf.mu.Unlock()
		time.Sleep(time.Millisecond*time.Duration(rf.heartbeat))//heartbeats
		rf.append_entries2all()
	}
}
//The receiver part of leader append
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//reset electiontimeout
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		// Debug(dTerm,"S%d reject S%d's append due to stale issue",rf.me,args.LeaderId)
		reply.Term = rf.CurrentTerm
		return 
	}else{
		rf.Convert2follower(args.Term) //it will update current Term & reset elhe_timer
	}
	if len(rf.Log)-1<args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(rf.Log)-1<args.PrevLogIndex{
			// Debug(dWarn,"S%d's log is less then PrevLogIndex%d\n",rf.me,args.PrevLogIndex)
			reply.ConflictIndex = len(rf.Log)	 //fast rollback
			reply.ConflictTerm  =  -1
		}else{
			// Debug(dWarn,"S%d's logTerm at %d is different from S%d's logTerm\n",rf.me,args.PrevLogIndex,args.LeaderId)
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
			for idx:= args.PrevLogIndex;idx>0&&rf.Log[idx].Term==reply.ConflictTerm;idx--{
				reply.ConflictIndex = idx
			}
		}		
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return 
	}	
	// valid append entries
	if args.Entries != nil{
		offset := args.PrevLogIndex+1
		for index, entry := range args.Entries {
			if index+offset >= len(rf.Log) || rf.Log[index+offset].Term != entry.Term {
				//confliction detected
				rf.Log = rf.Log[:index+offset]
				rf.Log = append(rf.Log, args.Entries[index:]...)
				rf.persist()
				break
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = Min(args.LeaderCommit,args.PrevLogIndex+len(args.Entries))
		Debug(dCommit,"S%d commit log%d",rf.me,rf.commitIndex)
		rf.cond.Broadcast()
	}
	reply.Success = true
	// Debug(dInfo,"S%d accept S%d's append/heartbeat request\n",rf.me,args.LeaderId)	
	reply.Term = rf.CurrentTerm
	return 
}

//--------------------------------------------------persistence-----------------------------------------------------
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor		int
	var Log					[]logEntry
	if d.Decode(&CurrentTerm) != nil ||
	   d.Decode(&VotedFor) != nil ||d.Decode(&Log) !=nil{
	  Debug(dError,"S%d readPersist error",rf.me)
	} else {
	  rf.CurrentTerm = CurrentTerm
	  rf.VotedFor = VotedFor
		rf.Log   = Log
	}
}

//--------------------------------------------snapshot-----------------------------------------------------
//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
//----------------------------------------------apply to client(state machine)----------------------------------------

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
func (rf *Raft)apply2sm(){
	for rf.killed() == false{
		rf.cond.L.Lock()
		rf.cond.Wait()
		//apply commited yet not applied to state machine 
		for	;rf.lastApplied<rf.commitIndex; {
			rf.lastApplied ++
			Debug(dClient,"S%d apply log%d(command %v,Term %d) to client",rf.me,rf.lastApplied,rf.Log[rf.lastApplied].Command,rf.Log[rf.lastApplied].Term)
			rf.applyCh <- ApplyMsg{true,rf.Log[rf.lastApplied].Command,rf.lastApplied,false,nil,0,0}
		}
		rf.cond.L.Unlock()
	}		
} 
//------------------------------------------------APIs--------------------------------------------------------------
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.mu.Lock()
	// Your initialization code here (2A, 2B, 2C).
  rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeat = 60
	//since the frist index is 1,so we append a dumb log entry at index 0
	rf.Log = []logEntry{}
	rf.Log = append(rf.Log,logEntry{nil,0})
	rf.elhe_timer = time.Now()
	rf.role = follower_
	rf.electionTimeout  = NewElection_timeout()
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())	
	init_debugger()
	// start election goroutine to start elections
	go rf.election()
	// apply committed log to state machine
	go rf.apply2sm()
	// leader announce the useful log to append
	go rf.leader_announce()
	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	isLeader := (rf.role == leader_)
	if isLeader{
		rf.Log = append(rf.Log,logEntry{command,rf.CurrentTerm})
		index = len(rf.Log)-1
		term = rf.CurrentTerm
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.role==leader_)
	return term, isleader
}