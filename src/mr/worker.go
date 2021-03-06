package mr

import "fmt"
// import "math/rand"
import "log"
import "net/rpc"
import "net"
import "net/http"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "time"
import "strings"
import "bufio"
 
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Workor struct {
	id int
	sockName string
}

type TempFiles struct {
	content string
	realFileName string
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

func AckCompletion() {
	completionRequest := CompletionRequest{}
	completionRequest.WorkerSock = workerSock(0)
	reply := CompletionReply{}

	call("Coordinator.TaskCompletion", &completionRequest, &reply)
	
} 


func AskForWork() WorkReply {
	args := WorkRequest{workerSock(0)}
	
	reply := WorkReply{}

	call("Coordinator.WorkRequest", &args, &reply)

	return reply
}

// We can implement AskForWork to
// return a PLEASE_EXIT pseudo  task to exit out of this loop
// Since each worker has its own process, this should be ok.

// In other words, each worker will have a busy loop,
// constantly asking cooridnator for work


func (w * Workor) ProcessTask(mapf func(string,string)[]KeyValue, reducef func(string,[] string) string) {
	for {
		workReply := w.AskForWork()
		if workReply.TaskType == NO_TASK_AVAIL {
			time.Sleep(2 * time.Second);	// Sleep for two seconds. More sophiscated solution would be to handle it from coord side
		} else if workReply.TaskType == MAP_TASK {
			w.ProcMapTask(workReply.FileName, workReply.TaskNum,  workReply.Nreduce,mapf)
		} else if workReply.TaskType == REDUCE_TASK {
			w.ProcReduceTask(workReply.TaskNum, reducef)
		} else if workReply.TaskType == PLEASE_EXIT {
			fmt.Println("Worker being told to exit")
			break
		}
	}
}

func (w * Workor) AskForWork() WorkReply {
	args := WorkRequest{w.sockName}
	
	reply := WorkReply{}

	call("Coordinator.WorkRequest", &args, &reply)

	return reply
}

func (w * Workor) AccCompletion() {
	completionRequest := CompletionRequest{}
	completionRequest.WorkerSock = w.sockName
	reply := CompletionReply{}

	call("Coordinator.TaskCompletion", &completionRequest, &reply)
}



func (w * Workor) ProcMapTask(fileName string, taskNum int, nReduce int , mapf func(string,string) []KeyValue) {
	// fmt.Println("Reading ", fileName)
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

	buffer := make(map[string] * TempFiles)
	
	
	for _, ch := range mapRes {
		reduceTaskNum := ihash(ch.Key) % nReduce
		reduceDir := "./reduce-tasks-" + strconv.Itoa(reduceTaskNum); 

		if _, err := os.Stat(reduceDir); os.IsNotExist(err) {
			err := os.Mkdir(reduceDir, os.ModePerm)
			if err != nil {
				fmt.Println("Error creating directory ", reduceDir);
			}
		}
		
		interFileName :=  reduceDir + "/tmr-"+ strconv.Itoa(taskNum) + "-"+ strconv.Itoa(reduceTaskNum)
		realFileName := reduceDir + "/imr-"+ strconv.Itoa(taskNum) + "-"+ strconv.Itoa(reduceTaskNum)
		kvPair := ch.Key + " " + ch.Value + "\n"

		// This way, if we need to recover from a crash, we refrain from reading old files
		if buffer[interFileName] == nil {
			os.Remove(interFileName)
			os.Create(interFileName)
			buffer[interFileName] = &TempFiles{}
		}

		buffer[interFileName].content += kvPair
		buffer[interFileName].realFileName = realFileName

		if len(buffer[interFileName].content) > 100 {
			f, err := os.OpenFile(interFileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}

			f.WriteString(buffer[interFileName].content)
			buffer[interFileName].content = "" // Reset buffer
			f.Close()
		} 
	 }
	 
	 // The idea here is that, once we have completed writing the strings to buffer,
	 // we atomically rename the temp file to avoid reading from partially complete files
	 for tmpFileName, tmpFileStruct := range buffer {
		f, err := os.OpenFile(tmpFileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		f.WriteString(tmpFileStruct.content)
		f.Close()
		os.Rename(tmpFileName, tmpFileStruct.realFileName)
	 }
 
	 w.AccCompletion()
}

// Relative un touched
// Trade offs:
//   At most R files
//  1. Collect the contents as we iterate over the files
func collecKvPairs(fileName string, kvPairs map [string] [] string) {
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
	}
	 
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		 
		words := strings.Fields(scanner.Text())
		kvPairs[words[0]] = append(kvPairs[words[0]], words[1])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	file.Close()
}

func (w * Workor) InitSessionWithCoord() {
		// declare an argument structure.
		args := HandShakeRequest{}

		// fill in the argument(s).
	
		// declare a reply structure.
		reply := HandShakeReply{}

		// send the RPC request, wait for the reply.
		call("Coordinator.HandShakeRequest", &args, &reply)
		
		// reply.Y should be 100.
		w.id = reply.WorkerId
	 	w.sockName = workerSock(w.id)
}

func (w * Workor) ProcReduceTask(taskNum int, reducef func(string, []string) string) {

	dirName := "./reduce-tasks-" + strconv.Itoa(taskNum)
	files, err := ioutil.ReadDir(dirName)

	// The problem is that sometimes we might not write to
	// a reduce id, because the we do not hash to that value
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
	   // path/to/whatever does not exist
	   w.AccCompletion()
	   return
	}
	if err != nil {
		log.Fatal(err)
	}


	kvPairs := make(map[string][]string)


	for _, f := range files {
	   pathName := dirName + "/" + f.Name()
	   collecKvPairs(pathName, kvPairs)
	}

	realFileName := "./mr-out-" + strconv.Itoa(taskNum)
	tmpFileName := "./tmr-out-" + strconv.Itoa(taskNum)
	os.Remove(tmpFileName)	// Remove the file before we create it, in case it already exists
 
	f, err := os.Create(tmpFileName)
	 
	for key, value := range kvPairs {
		 
		res := reducef(key, value)

		fmt.Fprintf(f, "%v %v\n", key, res);	
   }

   os.Rename(tmpFileName, realFileName)	  
   os.RemoveAll(dirName)
   f.Close()
	 
   w.AccCompletion()
}
 
// Sample responses from  worker
func (w *Workor) ReqFromServer(args * TaskDoneRequest, reply *TaskDoneReply) error {

	fmt.Println("Map Reduce job complete! ") 
	return nil
}

func(w *Workor) server() {
	rpc.Register(w);
	rpc.HandleHTTP()

	sockName := w.sockName
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("Listen error: ", e)
	}
	go http.Serve(l, nil)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	w := Workor{}
	w.InitSessionWithCoord()
	w.server()
	w.ProcessTask(mapf, reducef)
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
