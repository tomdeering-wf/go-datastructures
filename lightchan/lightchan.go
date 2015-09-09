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

	// Try sending the given item on the channel, returning the result without blocking
	// Sending may be unsuccessful if:
	//    (1) Another thread is currently modifying the channel
	//    (2) The channel lacks sufficient capacity (and hence a block would be necessary)
	// The send succeeded IFF access && capacity
	TrySend(item interface{}) (access, capacity bool)

	// Try sending the given item on the channel, returning success status without blocking
	// Receiving may be unsuccessful if:
	//    (1) Another thread is currently modifying the channel
	//    (2) No items are available to take (and hence a block would be necessary)
	// The receive succeeded IFF access && capacity
	TryReceive() (item interface{}, access, capacity bool)
}

// Constructs a new LightChan with the given capacity and zero-value.
// The zero value is the value with which to overwrite free space in the backing buffer, and should be the
// zero value of whatever type will be stored in this channel
func New(zeroValue interface{}, capacity int) LightChan {
	if capacity <= 0 {
		panic(fmt.Sprintf("Expected LightChan capacity > 0 but requested was %d!", capacity))
	}
	return &lightChan{
		capacity:  uint32(capacity),
		length:    0,
		isLocked:  falseUint,
		items:     make([]interface{}, capacity),
		head:      0,
		tail:      0,
		zeroValue: zeroValue,
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

	// Number of elements N currently stored in the channel, 0 <= N <= capacity
	length uint32

	// Whether the channel can be accessed
	isLocked uint32

	// The actual elements
	items []interface{}

	// The index of the first available item
	head uint32

	// The index where the next item should be stored
	tail uint32

	// The zeroed value of the type being stored
	zeroValue interface{}
}

func (lc *lightChan) TrySend(item interface{}) (access, capacity bool) {
	// Try to acquire the "lock"
	access = atomic.CompareAndSwapUint32(&lc.isLocked, falseUint, trueUint)

	// We've got exclusive access!
	if access {
		capacity = lc.length < lc.capacity
		if capacity {
			lc.items[lc.head] = item
			lc.head = (lc.head + 1) % lc.capacity
			lc.length = lc.length + 1
		}

		atomic.StoreUint32(&lc.isLocked, falseUint)
	}

	return access, capacity
}

func (lc *lightChan) TryReceive() (item interface{}, access, capacity bool) {
	// Try to acquire the "lock"
	access = atomic.CompareAndSwapUint32(&lc.isLocked, falseUint, trueUint)

	// We've got exclusive access!
	if access {
		capacity = lc.length > 0
		if capacity {
			item = lc.items[lc.tail]
			lc.items[lc.tail] = lc.zeroValue
			lc.tail = (lc.tail + 1) % lc.capacity
			lc.length = lc.length - 1
		}

		atomic.StoreUint32(&lc.isLocked, falseUint)
	}

	return item, access, capacity
}

func (lc *lightChan) Send(item interface{}) {
	// Sleep time to resolve concurrent channel contention
	accessSleep := time.Duration(0)

	// Keep trying until success
	for access, capacity := lc.TrySend(item); !access || !capacity; access, capacity = lc.TrySend(item) {
		// No access because of contention
		if !access {
			// First time losing a race
			if accessSleep == 0 {
				// Generate a small random sleep 1ns <= S <= 10ns
				accessSleep = time.Duration(rand.Intn(10) + 1)
			} else {
				// Each subsequent time we lose the race, double our sleep
				accessSleep = accessSleep * 2
			}
			// Sleep, then try again
			time.Sleep(accessSleep)
		} else {
			// Reset access sleep since we had no problem with access
			accessSleep = 0
			// Wait for more capacity to be available, then try again
			time.Sleep(capacityPollingInterval)
		}
	}

	// Successl
}

func (lc *lightChan) Receive() interface{} {
	// Sleep time to resolve concurrent channel contention
	accessSleep := time.Duration(0)

	var received interface{}
	var access, capacity bool
	// Keep trying until success
	for received, access, capacity = lc.TryReceive(); !access || !capacity; received, access, capacity = lc.TryReceive() {
		// No access because of contention
		if !access {
			// First time losing a race
			if accessSleep == 0 {
				// Generate a small random sleep 1ns <= S <= 10ns
				accessSleep = time.Duration(rand.Intn(10) + 1)
			} else {
				// Each subsequent time we lose the race, double our sleep
				accessSleep = accessSleep * 2
			}
			// Sleep, then try again
			time.Sleep(accessSleep)
		} else {
			// Reset access sleep since we had no problem with access
			accessSleep = 0
			// Wait for more capacity to be available, then try again
			time.Sleep(capacityPollingInterval)
		}
	}

	return received // Success
}

func (lc *lightChan) Len() int {
	return int(atomic.LoadUint32(&lc.length))
}

func (lc *lightChan) Cap() int {
	return int(lc.capacity)
}
