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
	return &lightChan{
		capacity:        uint32(capacity),
		length:          0,
		items:           make([]interface{}, capacity),
		head:            0,
		tail:            0,
		provisionalHead: 0,
		provisionalTail: 0,
		zeroValue:       zeroValue,
	}
}

const (
	// How long to wait before re-checking the capacity
	capacityPollingInterval = 1 * time.Nanosecond
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

	// Number of elements N currently stored in the channel, 0 <= N <= capacity
	length uint32

	// The actual elements
	items []interface{}

	// The index of the first available item
	head uint32

	// The index where the next item should be stored
	tail uint32

	provisionalHead uint32

	provisionalTail uint32

	// The zeroed value of the type being stored
	zeroValue interface{}
}

func (lc *lightChan) Send(item interface{}) {
	// Until success
	for {
		if lc.length == lc.capacity {
			// Wait for more capacity to be available, then try again
			time.Sleep(capacityPollingInterval)
		} else if atomic.CompareAndSwapUint32(&lc.provisionalHead, lc.head, (lc.head+1)%lc.capacity) {
			// We succeeded in moving the provisional head index, which means we're the exclusive next writer
			lc.items[lc.provisionalHead] = item
			atomic.AddUint32(&lc.length, 1)
			lc.head = lc.provisionalHead
			return
		}
	}
}

func (lc *lightChan) Receive() interface{} {
	// Until success
	for {
		if lc.length == 0 {
			// Wait for more capacity to be available, then try again
			time.Sleep(capacityPollingInterval)
		} else if atomic.CompareAndSwapUint32(&lc.provisionalTail, lc.tail, (lc.tail+1)%lc.capacity) {
			// We succeeded in moving the provisional tail index, which means we're the exclusive next reader
			item := lc.items[lc.provisionalTail]
			atomic.AddUint32(&lc.length, ^uint32(0))
			lc.tail = lc.provisionalTail
			return item
		}
	}
}

func (lc *lightChan) Len() int {
	return int(atomic.LoadUint32(&lc.length))
}

func (lc *lightChan) Cap() int {
	return int(lc.capacity)
}
