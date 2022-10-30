package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskReq struct {}
type TaskFinish struct{
	TASKTYPE string //"map","reduce"
	TASKID int 
} 
type TaskReply struct {
	TASKTYPE string //"map","reduce","wait","close"
	TASKID int
	FILENAME string  //only for "map" task
	NREDUCE int //workers need this information
	NMAP int //workers need this information
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
