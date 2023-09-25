package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(debugLevels string, format string, a ...interface{}) (n int, err error) {
	if Debug && debugLevels == LOG_LEVEL_ELECTION {
		log.Printf(format, a...)
	}
	return
}

func RandRange(min, max int) int {
    if min >= max {
        return min
    }
    rand.Seed(time.Now().UnixNano())
    return min + rand.Intn(max-min)
}

func GetServerState(serverState int) (string){
	if serverState == Follower {
		return "Follower"
	}

	if serverState == Candidate {
		return "Candidate"
	}

	return "Leader"
}

func min(a int, b int) int {
	if (a > b ) {
		return b
	}
	return a
}