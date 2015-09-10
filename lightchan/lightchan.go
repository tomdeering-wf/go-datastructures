/*
Copyright 2015 Workiva Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// LightChan is a fast, buffered alternative to Go's built-in channels for the most common producer-consumer use cases.
//
// Use Case:
// - M producers and N consumers, where M, N > 0
// - Fixed capacity for the backing buffer is known a-priori
// - Thread-safe operations are desired
//
// Blocking access is accomplished via lightweight polling with exponential backoff if thread contention is detected.
//
// To receive a value in a select statement using Go's syntactic sugar for channels:
//
// lc := lightchan.New(nil, 50)
// ...
// sugarChan := make(chan *myStruct, 1)
// go func(){sugarChan <- lc.Receive()}()
// select{
// 	case item := <- sugarChan:
// 		// Do something
// }

package lightchan

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

type LightChan interface {
	// Send the given item on the channel, blocking until buffer space is available
	Send(item interface{})

	// Receive the next value on the channel, blocking until it is available
	Receive() interface{}

	// Returns the number of elements queued in the channel buffer
	Len() int

	// Returns the capacity of the channel buffer
	Cap() int
}

// Constructs a new LightChan with the given capacity and zero-value.
// The zero value is the value with which to overwrite free space in the backing buffer, and should be the
// zero value of whatever type will be stored in this channel
func New(zeroValue interface{}, capacity int) LightChan {
	if capacity <= 0 {
		panic(fmt.Sprintf("Expected LightChan capacity > 0 but requested was %d!", capacity))
	}

	emptySlice := make([]interface{}, 0)
	return &lightChan{
		zeroValue: zeroValue,
		capacity:  uint32(capacity),
		bodyPtr:   unsafe.Pointer(&emptySlice),
	}
}

const (
	// How long to wait before re-checking the capacity
	capacityPollingInterval = 20 * time.Nanosecond
	trueUint                = uint32(1)
	falseUint               = uint32(0)
)

var (
	// Random number generator, used for generating random sleep seeds
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// Unexported implementation of LightChan
type lightChan struct {
	// Capacity of the underlying storage
	capacity uint32

	// The zeroed value of the type being stored
	zeroValue interface{}

	// The mutable body of the channel, a pointer to []interface{}
	bodyPtr unsafe.Pointer
}

func (lc *lightChan) Send(item interface{}) {
	// Sleep time to resolve concurrent channel contention
	accessSleep := time.Duration(0)

	// Keep trying until success
	for {
		cur := (*[]interface{})(lc.bodyPtr)

		// No room, wait for capacity
		if len(*cur) == int(lc.capacity) {
			time.Sleep(capacityPollingInterval)
			continue
		}

		// Speculative update
		newLen := len(*cur) + 1
		next := make([]interface{}, newLen)
		copy(next, *cur)
		next[newLen-1] = item

		// Try to update
		if atomic.CompareAndSwapPointer(&lc.bodyPtr, unsafe.Pointer(cur), unsafe.Pointer(&next)) {
			// Success!
			return
		}

		if accessSleep == 0 {
			// Generate a small random sleep  s, 1ns <= s <= 10ns
			accessSleep = time.Duration(rand.Intn(10) + 1)
		} else {
			// Each subsequent time we lose the race, double our sleep
			accessSleep = accessSleep * 2
		}
		time.Sleep(accessSleep)
	}
}

func (lc *lightChan) Receive() interface{} {
	// Sleep time to resolve concurrent channel contention
	accessSleep := time.Duration(0)

	// Keep trying until success
	for {
		cur := (*[]interface{})(lc.bodyPtr)

		// No items, wait for capacity
		if len(*cur) == 0 {
			time.Sleep(capacityPollingInterval)
			continue
		}

		// Speculative update
		newLen := len(*cur) - 1
		next := make([]interface{}, newLen)
		for i := 1; i < len(*cur); i++ {
			next[i-1] = (*cur)[i]
		}
		item := (*cur)[0]

		// Try to update
		if atomic.CompareAndSwapPointer(&lc.bodyPtr, unsafe.Pointer(cur), unsafe.Pointer(&next)) {
			// Success!
			return item
		}

		if accessSleep == 0 {
			// Generate a small random sleep  s, 1ns <= s <= 10ns
			accessSleep = time.Duration(rand.Intn(10) + 1)
		} else {
			// Each subsequent time we lose the race, double our sleep
			accessSleep = accessSleep * 2
		}
		time.Sleep(accessSleep)
	}
}

func (lc *lightChan) Len() int {
	return len(*(*[]interface{})(lc.bodyPtr))
}

func (lc *lightChan) Cap() int {
	return int(lc.capacity)
}
