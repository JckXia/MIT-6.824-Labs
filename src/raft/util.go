package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
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