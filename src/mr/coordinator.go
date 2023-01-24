package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"

type Task struct{
	task_type string  //"map" or "reduce"
	task_state string //"idle","running" or "finished"
	task_id int //index into map_tasks or reduce_tasks
	start_time time.Time
}

type Coordinator struct {
	mu sync.Mutex  //mutual exclusion
	filenames []string //all files that need map
	nMap int //number of map tasks
	nReduce int //number of reduce tasks
	map_tasks []Task //one map task for an input file
	reduce_tasks []Task
}

//
// the RPC argument and reply types are defined in rpc.go.
//

//search for an idle map_task
//if we find an idle map_task, we return (idle_task,false)
//if there is no idle map_task:
//1. there are some runnning tasks,we return(nil,false)
//2. all map tasks are finished, we return (nil,true)
func (c *Coordinator) assign_map() (Task,bool){
	all_done := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for i:=0;i<c.nMap;i++ {
		if c.map_tasks[i].task_state == "idle"{
			c.map_tasks[i].task_state = "running"
			c.map_tasks[i].start_time = time.Now()
			return c.map_tasks[i],false
		}else if c.map_tasks[i].task_state == "running" {
			if time.Since(c.map_tasks[i].start_time) / time.Second > 10{
				c.map_tasks[i].start_time = time.Now()
				return c.map_tasks[i],false
			}
			all_done = false
		}
	}
	return Task{"nothing","none",-1,time.Now()},all_done
}
//only called by assign_task if assign_map return (nil,true),that is all map tasks are finished
//search for an idle reduce_task
//if we find an idle reduce_task, we return (idle_task,false)
//if there is no idle reduce_task:
//1. there are some runnning tasks,we return(nil,false)
//2. all reduce tasks are finished, we return (nil,true)
func (c *Coordinator) assign_reduce() (Task,bool){
		all_done := true
		c.mu.Lock()
		defer c.mu.Unlock()
		for i:=0;i<c.nReduce;i++ {
			if c.reduce_tasks[i].task_state == "idle"{
				c.reduce_tasks[i].task_state = "running"
				c.reduce_tasks[i].start_time = time.Now()
				return c.reduce_tasks[i],false
			}else if c.reduce_tasks[i].task_state == "running"{
				if time.Since(c.reduce_tasks[i].start_time) / time.Second > 10{
					c.reduce_tasks[i].start_time = time.Now()
					return c.reduce_tasks[i],false
				}
				all_done = false
			}
		}
		return Task{"nothing","none",-1,time.Now()},all_done
}

func (c *Coordinator) Assign_task(args *TaskReq, reply *TaskReply) error {
	map_task,all_done := c.assign_map()
	if map_task.task_type!= "nothing" {
		reply.TASKTYPE = "map"
		reply.TASKID = map_task.task_id
		reply.NREDUCE = c.nReduce
		reply.FILENAME = c.filenames[map_task.task_id-1]
		reply.NMAP = c.nMap
		// fmt.Println("Dispatch map task ",map_task.task_id)
	} else if all_done == false{
		reply.TASKTYPE = "wait"
	}else{
		//all map tasks are finished, we can begin reduce tasks
		reduce_task,all_done := c.assign_reduce()
		if reduce_task.task_type!="nothing" {
			reply.TASKTYPE = "reduce"
			reply.TASKID = reduce_task.task_id
			reply.NREDUCE = c.nReduce
			reply.FILENAME = "no need"
			reply.NMAP = c.nMap
			// fmt.Println("Dispatch reduce task ",reduce_task.task_id)
		} else if all_done == false{
			reply.TASKTYPE = "wait"
		} else{
			reply.TASKTYPE = "close"
		}
	}
	return nil
}

func (c *Coordinator) Finish_task(args *TaskFinish, reply *TaskReply) error {
	if args.TASKTYPE == "map"{
		if args.TASKID < 1 || args.TASKID >c.nMap{
			fmt.Println("illegal map_task id ",args.TASKID)
		}else{
			c.mu.Lock()
			c.map_tasks[args.TASKID-1].task_state = "finished"
			c.mu.Unlock()
		}	
	}else if args.TASKTYPE == "reduce"{
		if args.TASKID < 1 || args.TASKID >c.nReduce{
			fmt.Println("illegal reduce_task id ",args.TASKID)
		}else{
			c.mu.Lock()
			c.reduce_tasks[args.TASKID-1].task_state = "finished"
			c.mu.Unlock()
		}	
	}else{
		fmt.Println("illegal finish operation")
	}
	return nil
}
//generate map task for each input file 
//generate nReduce reduce task 
func (c *Coordinator) generate_tasks() {
	for i:=0;i<c.nMap;i++{
		c.map_tasks=append(c.map_tasks,Task{"map","idle",i+1,time.Now()})
	}
	for i:=0;i<c.nReduce;i++{
		c.reduce_tasks=append(c.reduce_tasks,Task{"reduce","idle",i+1,time.Now()})
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _,reduce_task := range(c.reduce_tasks){
		if reduce_task.task_state != "finished"{
			ret = false
		}
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.filenames = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.generate_tasks()

	c.server()
	return &c
}
