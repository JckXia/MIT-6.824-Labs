package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"


/**
	Task 1:
	Set up RPC connection between coordinator and worker so that
	- Worker can request a task to coordinator.
	- Coordinator, upon recv, checks for yet-to-do map tasks and
      send to worker
	- Worker receives fileName, calls Map function provided by wc.so

**/

// Coordinator will have some global state
type Coordinator struct {
	
	mu   sync.Mutex

	mapTasks map[int]string
	mMapCnt int

	nReduceTasks map[int] string
	nReduceCnt int
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

func (c *Coordinator) WorkRequest(args *WorkRequest, reply * WorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mMapCnt >= 0{;
		fmt.Println("c map count ", c.mMapCnt)
		reply.MapTaskNum = c.mMapCnt
		reply.FileName = c.mapTasks[c.mMapCnt]
		reply.Status = 200 

		c.mMapCnt--; //TODO I think we should only invoke this when map op finishes
	} else {
		reply.MapTaskNum = 0
		reply.FileName = ""
		reply.Status = 500
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
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

 
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

// Problem: 

//	How to control worker state? 
// 	Crash recovery?
//  How to coordinate multiple workers?

//  What do Reduce workers do?

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu.Lock()
	c.mapTasks = make(map[int] string)
	c.mMapCnt = -1;
	
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = files[i]
		c.mMapCnt++
	}
 
	c.nReduceCnt = nReduce

	c.mu.Unlock()

	c.server() 
	return &c
}
