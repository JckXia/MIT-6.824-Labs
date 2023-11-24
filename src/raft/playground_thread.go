package main

// Import the sync package
import (
	"fmt"
	"sync"
	"time"
)

// Conditionv ariable keeps a mutex reserved for the cond var
// Keep a `condition` condition variable
// Keep the `sharedData` as the protected value
// done as some sort of guard
var (
	mutex      sync.Mutex
	condition  *sync.Cond
	sharedData int
	done       bool
)

type RaftP struct {
	commitIndex int
}


func main() {

	t := RaftP{2}
 
	// Create a new condition variable
	condition = sync.NewCond(&mutex)

	// Start a goroutine to modify shared data
	// go modifySharedData()
	go t.RaftUpdateCommitIndex()
	// Wait for the shared data to reach a certain value
	// waitForValue(10)
	t.applyChThread()

	fmt.Println("Main goroutine: Received the expected value, exiting...")
}

func (r * RaftP) RaftUpdateCommitIndex() {
	for i :=1;i <= 15; i++ {
		time.Sleep(time.Millisecond * 200)
		mutex.Lock()
		
		r.commitIndex = i
		mutex.Unlock()
		r.signalCommitIndexUpdate()
	}
}

func (r *RaftP) signalCommitIndexUpdate() {
	mutex.Lock()
	defer mutex.Unlock()
	condition.Signal()
}

func (r *RaftP) applyChThread() {
	mutex.Lock()
	defer mutex.Unlock()
	for {

	for r.commitIndex != 10 {
		fmt.Printf("commit INdex %d is not 10, waiting...\n", r.commitIndex)
		condition.Wait()
		fmt.Printf("Woken up with new value %d\n", r.commitIndex)
	}
 

	fmt.Println("We are done!")
	fmt.Println(r.commitIndex)
	break
	}	
}
 
 

func notifyCondition() {
	mutex.Lock()
	defer mutex.Unlock()
	condition.Signal()
}

func signalDone() {
	mutex.Lock()
	defer mutex.Unlock()
	done = true
	condition.Broadcast()
}
