package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	REDUCE_TASK = "ReduceTask"
	MAP_TASK = "MapTask"
	NO_TASK_AVAIL = "NoTaskAvail"
)


type WorkRequest struct {
	WorkSock string
}

type WorkReply struct {
	TaskNum int
	FileName string
	TaskType string
}

// TaskNum 
type CompletionRequest struct {
	TaskNum int
}

type CompletionReply struct {
	Status int
}


//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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


func workerSock() string {
	s := "/var/tmp/824-mrw-";
	s += strconv.Itoa(os.Getuid())
	return s
}