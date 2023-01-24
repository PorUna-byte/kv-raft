package raft
import "log"
import "time"
import "os"
import "fmt"
import "strconv"
import "sync"
//--------------------------------------------------- Max & Min ----------------------------------------------------------
func Max(a int,b int) int{
	if a>b{
		return a
	}else{ 
		return b
	}	
}
func Min(a int,b int) int{
	if a<b{
		return a
	}else{ 
		return b
	}	
}
//------------------------------------------------- Debug ----------------------------------------------------------------
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
type logTopic string
const (
  candidate_ string = "candidate"
	follower_  string = "follower"
	leader_    string = "leader" 
	
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init_debugger() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		// if topic==dTerm||topic==dVote{
			prefix := fmt.Sprintf("%06d %v ", time, string(topic))
			format = prefix + format
			log.Printf(format, a...)
		// }
	}
}
//----------------------------------------------------utils----------------------------------------------------------
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

func (rf *Raft) get_lastlogindex() int{
	return rf.Log[len(rf.Log)-1].LogIndex
}
func (rf *Raft) get_lastlogTerm() int{
	return rf.Log[len(rf.Log)-1].Term
}
func (rf *Raft) get_snapincludedindex() int{
	return rf.Log[0].LogIndex
}
func (rf *Raft) get_snapincludedterm() int{
	return rf.Log[0].Term
}
func (rf *Raft) logindex2sliceindex(logindex int) int{
	return logindex-rf.get_snapincludedindex()
}
func (rf *Raft) sliceindex2logindex(sliceindex int) int{
	return sliceindex+rf.get_snapincludedindex()
}