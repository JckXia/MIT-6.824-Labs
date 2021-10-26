package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"


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

// We can have two channels
// a reduceTask channel
// a mapTask channel


// Next steps:
// 1. ReWrite WorkRequest logic using
// 	  the channels, make sure everything works
//	  (This part of the logic shouldn't concern the workers)

// 2. Add error handling logic in the timers.

// Now, we only start reduceTask after mapTask completes
// concerns:
// WorkRequests dequeues from the map queue, one at a time
//  	1. If it succeeds, then that task is gone forever
//		2. If by the end of the workerTimer (when it wakes up) and workeker
//		   is still mapping, write the taskNumber to the map channel

// If all map task completed, we start reading from the reduce task queue.
// Work request deque from the reduce task queue, one at a time.
//		1. If it succeeds, task is gone forever
//		2. If by end of workertimer, and worker is still reducing, write
//			taskNumber to the reduce channel.
 

// ReWrite:
// mapTasks map a taskNum to  struct 
//						        Status     "In Progress" || "Completed"
//								WorkerSock  string

// When coordinator receives Acc report, set Status to Completed. Reset WorkerSock

//
type TaskStatus struct {
	Status string
	WorkerSock string
	FileName string
}
type Coordinator struct {
	
	mu   sync.Mutex

	workerStatus map[string] *WorkerStatus

	mapTask map[int] *TaskStatus
	mapTasks map[int]string
	mMapAvailCnt int

	mMapCompleteCnt int
	mMap int

	nReduceTasks map[int] string
	nReduceTask map[int] *TaskStatus
	nReduceAvailCnt int

	nReduceCompleteCnt int
	nReduce int

	workerIds int

	mapChan chan int
	reduceChan chan int
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
		c.workerStatus[workerSock].Status = "Idle"
		c.mapTask[workerInfo.TaskNum].Status = "Complete"
		c.mMapCompleteCnt++
	} else if workerInfo.Status == "Reducing" {
		c.workerStatus[workerSock].Status = "Idle"
		c.nReduceTask[workerInfo.TaskNum].Status = "Complete"
		c.nReduceCompleteCnt++
	} else {
		// DO NOTHING
	}
	reply.Status = 200
	
	return nil
}

func workerTimer(c *Coordinator, workSock string) {
	time.Sleep(2 * time.Second)
	fmt.Println("Task Sock ", workSock)
}

// Testing plan
// First check that we are able to delegate tasks correctly to map workers,
// then reducers as following
func (c *Coordinator) WorkRequest(args *WorkRequest, reply * WorkReply) error {
 
 	
	mapTaskNum, channelOpen := <-c.mapChan
	 
	// Channel is closed
	if channelOpen {
		// we need to mark tests
		c.mu.Lock()
		defer c.mu.Unlock()
		fmt.Println("Check! ", channelOpen)

 
		reply.TaskNum = mapTaskNum
		reply.FileName = c.mapTask[mapTaskNum].FileName
		reply.TaskType = MAP_TASK
		reply.Nreduce = c.nReduce

		c.workerStatus[args.WorkSock] = &WorkerStatus{reply.TaskNum, "Mapping"}
		c.mapTask[mapTaskNum].Status = "Mapping"
		c.mapTask[mapTaskNum].WorkerSock = args.WorkSock
		fmt.Println("Task num ", mapTaskNum) 
 
	} else {
		
		fmt.Println("Processing reduce work")
		// TODO: Need to collect reduce tasks to delegate 
		reduceTaskNum, channelOpen := <- c.reduceChan
		if channelOpen {
		  c.mu.Lock()
		  defer c.mu.Unlock()

		  reply.TaskNum = reduceTaskNum
		  reply.FileName = ""
		  reply.TaskType = REDUCE_TASK
		  reply.Nreduce = c.nReduce
		  c.workerStatus[args.WorkSock] = &WorkerStatus{reply.TaskNum,"Reducing"}
		  c.nReduceTask[reduceTaskNum].Status = "Reducing"
		  c.nReduceTask[reduceTaskNum].WorkerSock = args.WorkSock
		   
		} else {
			reply.TaskType = PLEASE_EXIT
		}

		return nil
	}
		
	return nil
} 

func callWorker(rpcname string, args interface{}, reply interface {}, sockName string) bool {
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("Dialing", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

func (c * Coordinator) reduceComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, taskStatus := range c.nReduceTask {
		if taskStatus.Status != "Complete" {
			 return false
		}
	}
	return true
}

func (c * Coordinator) mapComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, taskStatus := range c.mapTask {
		if taskStatus.Status != "Complete" {
			 return false
		}
	}
	return true
}

func (c * Coordinator) getTasks() {

	for mapTaskNum, taskStatus := range c.mapTask {
		if taskStatus.Status == "Availlable" {
			
			c.mapChan <- mapTaskNum
			 
		}
	}
	
	// Need to poll until mapTaskComplete is true
	// Should be..okay? Since we are adding the queue from the timer worker	 
	for !(c.mapComplete()) {
		 
	}
	close(c.mapChan)
	
	for i := 0; i< c.nReduce ; i++ {
		c.reduceChan <- i
	}

 
	fmt.Println("Map work complete!")

	for !(c.reduceComplete()) {

	}

	close(c.reduceChan)
	c.nReduceCompleteCnt = c.nReduce 
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
	go c.getTasks()
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

// Potential solution is to clean up the reduce-tasks-* 
// directory here.
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
	c.workerStatus = make(map[string] *WorkerStatus)
    
	c.mapTask = make(map[int]* TaskStatus)
	c.nReduceTask = make(map[int] *TaskStatus)

	c.mapTasks = make(map[int] string)

	c.mapChan = make(chan int)
	c.reduceChan = make(chan int)

	c.mMapAvailCnt = -1;
	c.workerIds = 0;

	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = files[i]
		c.mapTask[i] = &TaskStatus {"Availlable", "", files[i]}
		c.mMapAvailCnt++
	}

	// TODO: Need to have the map workers report the reduce steps.
	// For now simply load it into nReduceTask
	for i :=0;i< nReduce; i++ {
		//c.nReduceTasks[i] = files[i]
		 c.nReduceTask[i] = &TaskStatus{"Availlable","", ""}
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
