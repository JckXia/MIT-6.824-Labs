package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"


/**
	Task 1 : (Done)
	Set up RPC connection between coordinator and worker so that
	- Worker can request a task to coordinator.
	- Coordinator, upon recv, checks for yet-to-do map tasks and
      send to worker
	- Worker receives fileName, calls Map function provided by wc.so 

	Task 2:
	- Ability to spin up one worker and one coordinator, and that it is able to carry out a map reduce job by itself
	- Reduce jobs should sleep periodically to check that all map tasks are done
	- Coordinator should be able to call terminate 

	Task 3:
	- Ability to spin up multiple workers and check to see that it is able to carry out map reduce jobs (multiple)

	Task 4:
	- Error recovery
	- Ability for coordinator to contact workers, (refactor out worker as a rpc server)

	map[socketName] = {
		TaskNum 
		Status = "Mapping" | Redicomg | Idle
	}
**/

// Coordinator will have some global state
// TODO Can probably simply this logic quite a bit. 

// Worker sock /var/tmp/st  

type WorkerStatus struct {
	TaskNum int
	Status string
}

type Coordinator struct {
	
	mu   sync.Mutex

	workerStatus map[string] WorkerStatus

	mapTasks map[int]string
	mMapAvailCnt int

	mMapCompleteCnt int
	mMap int

	nReduceTasks map[int] string
	nReduceAvailCnt int

	nReduceCompleteCnt int
	nReduce int

	workerIds int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// TODO add a system for recycling worker ids
func (c *Coordinator ) HandShakeRequest(args* HandShakeRequest, reply * HandShakeReply) error{
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = c.workerIds 
	c.workerIds++

	return nil;
}

// TODO add error handling at ack. (Potentially could fail for what ever reason)
func (c *Coordinator) TaskCompletion(args *CompletionRequest, reply *CompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	workerSock := args.WorkerSock
	workerInfo := c.workerStatus[workerSock]

	// The problem is that, becasue we are delegating worker based on their workerSock,
	// which we had assumed was unique, we have effectively under counted when they return 

	if workerInfo.Status == "Mapping" {
		c.mMapCompleteCnt++
	} else if workerInfo.Status == "Reducing" {
		c.nReduceCompleteCnt++
	} else {
		// DO NOTHING
	}
	reply.Status = 200
	
	return nil
}

func (c *Coordinator) WorkRequest(args *WorkRequest, reply * WorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("tasks ", c.mMapCompleteCnt)
	if c.mMapAvailCnt >= 0{
		// We have map tasks availlable
		reply.TaskNum = c.mMapAvailCnt
		reply.FileName = c.mapTasks[c.mMapAvailCnt]
		reply.TaskType = MAP_TASK 
		reply.Nreduce = c.nReduce

		c.workerStatus[args.WorkSock] = WorkerStatus{reply.TaskNum,"Mapping"}
		c.mMapAvailCnt--; 
	} else if c.nReduceAvailCnt >= 0 && c.mMapCompleteCnt == c.mMap  {
		fmt.Println("Assigining reducer work ")
		// At this point, we have handed out all map tasks. 
		// But we must also complete all map tasks before we can give out
		// reducer work
		reply.TaskNum = c.nReduceAvailCnt
		reply.FileName = ""
		reply.TaskType = REDUCE_TASK
		reply.Nreduce = c.nReduce
		
		c.workerStatus[args.WorkSock] = WorkerStatus{reply.TaskNum,"Reducing"}
		c.nReduceAvailCnt--;

	} else {

		reply.TaskNum = -1
		reply.FileName = ""
		reply.Nreduce = c.nReduce
 
		if c.mMapCompleteCnt == c.mMap && c.nReduceCompleteCnt == c.nReduce {
			reply.TaskType = PLEASE_EXIT
		} else {
			reply.TaskType = NO_TASK_AVAIL
		}
		// Either way, it is idling
		c.workerStatus[args.WorkSock] = WorkerStatus{reply.TaskNum, "Idle"}
	}
	
	return nil
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

// Perform completion logic here 
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()
	 
	ret := c.nReduceCompleteCnt == c.nReduce
	// Your code here.
	return ret
}

 
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

// Problem: 

//	How to control worker state?  (Current solution: Store worker sock in a map)
// 	Crash recovery?
//  How to coordinate multiple workers?
 

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu.Lock()
	c.workerStatus = make(map[string] WorkerStatus)

	c.mapTasks = make(map[int] string)
	c.mMapAvailCnt = -1;
	c.workerIds = 0;
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = files[i]
		c.mMapAvailCnt++
	}

	c.mMap = len(files)
	c.mMapCompleteCnt = 0


	c.nReduceAvailCnt = nReduce - 1
	c.nReduce = nReduce
	c.nReduceCompleteCnt = 0
	
	fmt.Println(nReduce, " reduce tasks")
	c.mu.Unlock()

	c.server() 
	return &c
}
