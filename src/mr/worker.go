package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


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
// Eventually we would need to spin up a server
//

// TODO all of these error messages MUST be handled by the coordinator in the futre
func ProcessMapTask(fileName string, taskNum int, nReduce int , mapf func(string,string) []KeyValue) {
	fmt.Println("Reading ", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Cannot read %v ", fileName)
	}

	fileContent, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("Cannot read %v ", fileName)
	}

	mapRes := mapf(fileName, string(fileContent))
	file.Close()

	buffer := make(map[string] string)

	for _, ch := range mapRes {
		reduceTaskNum := ihash(ch.Key) % nReduce
		interFileName := "imr-"+ strconv.Itoa(taskNum) + "-"+ strconv.Itoa(reduceTaskNum)
		kvPair := ch.Key + " " + ch.Value + "\n"
		buffer[interFileName] += kvPair
		if len(buffer[interFileName]) > 100 {
			f, err := os.OpenFile(interFileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}

			f.WriteString(buffer[interFileName])
			buffer[interFileName] = "" // Reset buffer
		} 
	 }

	 // Clear out any remainants
	 // Terrible solution!!
	 for fileName, fileContent := range buffer {
		f, err := os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		f.WriteString(fileContent)
	 }
	 AckCompletion()
}

// Find all files matching the
//		imr-*-Y.out pattern
//		Read contentS, Load into map<key, list(values)>
// 		iterate over map, load results into a 
//		mr-out-Y file, where y is the reduce task
func ProcessReduceTask(taskNum int, reducef func(string, []string) string) {
	 fmt.Println("Running reduce task ", taskNum)

	 AckCompletion()
}

// We can implement AskForWork to
// return a PLEASE_EXIT pseudo  task to exit out of this loop
// Since each worker has its own process, this should be ok.

// In other words, each worker will have a busy loop,
// constantly asking cooridnator for work

func ProcessTask(mapf func(string,string)[]KeyValue, reducef func(string,[] string) string) {
	
	for {
		workReply := AskForWork()
		if workReply.TaskType == NO_TASK_AVAIL {
			time.Sleep(2 * time.Second);	// Sleep for two seconds
		} else if workReply.TaskType == MAP_TASK {
			ProcessMapTask(workReply.FileName, workReply.TaskNum,  workReply.Nreduce,mapf)
		} else if workReply.TaskType == REDUCE_TASK {
			ProcessReduceTask(workReply.TaskNum, reducef)
		} else if workReply.TaskType == PLEASE_EXIT {
			fmt.Println("Worker being told to exit")
			break
		}
	}
}

func AckCompletion() {
	completionRequest := CompletionRequest{}
	completionRequest.WorkerSock = workerSock()
	reply := CompletionReply{}

	call("Coordinator.TaskCompletion", &completionRequest, &reply)
	
} 

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	ProcessTask(mapf, reducef)
	
}

func AskForWork() WorkReply {
	args := WorkRequest{workerSock()}

	reply := WorkReply{}

	call("Coordinator.WorkRequest", &args, &reply)

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
