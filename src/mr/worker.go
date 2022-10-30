package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "time"
import "strconv"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return ihash(a[i].Key)%10 < ihash(a[j].Key)%10 }

type ByrawKey []KeyValue
func (a ByrawKey) Len() int           { return len(a) }
func (a ByrawKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByrawKey) Less(i, j int) bool { return a[i].Key<a[j].Key}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	loop := true
	for ;loop;{
		time.Sleep(1)
		reply := Calltask()
		if reply.TASKTYPE == "map" {
			// fmt.Println("receive map task",reply.TASKID)
			input_file, err := os.Open(reply.FILENAME)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FILENAME)
			}
			content, err:= ioutil.ReadAll(input_file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FILENAME)
			}
			X := reply.TASKID //X is the map task number
			kva := mapf(reply.FILENAME, string(content))
			sort.Sort(ByKey(kva))
			input_file.Close()
			first_file := "mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(ihash(kva[0].Key)%reply.NREDUCE+1)
			intermediate_file,err := os.OpenFile(first_file, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
			if err != nil {
				log.Fatalf("cannot open first %v", first_file)
			}
			enc := json.NewEncoder(intermediate_file)
			enc.Encode(&kva[0])
			for i:=1;i<len(kva);i++{
				if ihash(kva[i].Key)%10 != ihash(kva[i-1].Key)%10 {
					Y := ihash(kva[i].Key)%reply.NREDUCE+1 //Y is the reduce task number.
					new_file := "mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(Y)
					intermediate_file,err = os.OpenFile(new_file, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666) 
					if err!= nil{
						log.Fatalf("cannot open %v", new_file)
					}
					enc = json.NewEncoder(intermediate_file)	
				}
				enc.Encode(&kva[i])
			}
			//wo don't use reply,we just notify the coordinator
			reply = finishtask(TaskFinish{reply.TASKTYPE,reply.TASKID}) 
		}else if reply.TASKTYPE == "reduce" {
			// fmt.Println("receive reduce task",reply.TASKID)
			intermediate := []KeyValue{}
			for i:=0;i<reply.NMAP;i++{
				filename := "mr-"+strconv.Itoa(i+1)+"-"+strconv.Itoa(reply.TASKID)
				file, err := os.Open(filename)
				if err!=nil{
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()	
			}
			//now intermediate contains all KV pair that need to write into mr-out-Y
			sort.Sort(ByrawKey(intermediate))
			ofile,_:=  os.OpenFile("mr-out-"+strconv.Itoa(reply.TASKID), os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			//wo don't use reply,we just notify the coordinator
			reply = finishtask(TaskFinish{reply.TASKTYPE,reply.TASKID}) 
		}else if reply.TASKTYPE == "wait"{
			time.Sleep(10)
		}else if reply.TASKTYPE == "close"{
			//we exit loop
			loop=false
		}
	}
}

func Calltask() (TaskReply){
	// declare an argument structure.
	args := TaskReq{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.assign_task" tells the
	// receiving server that we'd like to call
	// the assign_task() method of struct Coordinator.
	ok := call("Coordinator.Assign_task", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func finishtask(args TaskFinish) (TaskReply){
	// declare an argument structure.

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Finish_task" tells the
	// receiving server that we'd like to call
	// the Finish_task() method of struct Coordinator.
	ok := call("Coordinator.Finish_task", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
