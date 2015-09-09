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

package lightchan

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var smallItemSet = []int{-18, -20, 0, 3, 0, 6, 7, -100}

// Ensure that zero capacity panics
func TestZeroCapacityPanics(t *testing.T) {
	defer assertPanic(t)
	New(nil, 0)
}

// Ensure that zero capacity panics
func TestNegativeCapacityPanics(t *testing.T) {
	defer assertPanic(t)
	New(nil, -1)
}

// Ensure that the capacity is accurate and never changes
func TestCap(t *testing.T) {
	lc := New(0, len(smallItemSet))
	assert.Equal(t, len(smallItemSet), lc.Cap())
	for _, i := range smallItemSet {
		assert.Equal(t, len(smallItemSet), lc.Cap())
		lc.Send(i)
		assert.Equal(t, len(smallItemSet), lc.Cap())
	}
	for range smallItemSet {
		assert.Equal(t, len(smallItemSet), lc.Cap())
		lc.Receive()
		assert.Equal(t, len(smallItemSet), lc.Cap())
	}
	assert.Equal(t, len(smallItemSet), lc.Cap())
}

// Ensure that the length is accurate and gets incremented and decremented
func TestLen(t *testing.T) {
	lc := New(0, len(smallItemSet))
	assert.Equal(t, 0, lc.Len())
	for idx, i := range smallItemSet {
		assert.Equal(t, idx, lc.Len())
		lc.Send(i)
	}
	for idx, _ := range smallItemSet {
		assert.Equal(t, len(smallItemSet)-idx, lc.Len())
		lc.Receive()
	}
	assert.Equal(t, 0, lc.Len())
}

// Ensure that TrySend does not block and returns the correct values based on lock status and capacity
func TestTrySend(t *testing.T) {
	lc := New(0, 1)
	access, capacity := lc.TrySend(42)
	assert.True(t, access)
	assert.True(t, capacity)

	access, capacity = lc.TrySend(42)
	assert.True(t, access)
	assert.False(t, capacity)

	lc.(*lightChan).isLocked = trueUint
	access, capacity = lc.TrySend(42)
	assert.False(t, access)
	assert.False(t, capacity)
}

// Ensure that TryReceive does not block and returns the correct values based on lock status and capacity
func TestTryReceive(t *testing.T) {
	lc := New(0, 1)
	lc.Send(42)
	item, access, capacity := lc.TryReceive()
	assert.Equal(t, 42, item)
	assert.True(t, access)
	assert.True(t, capacity)

	item, access, capacity = lc.TryReceive()
	assert.Equal(t, nil, item)
	assert.True(t, access)
	assert.False(t, capacity)

	lc.(*lightChan).isLocked = trueUint
	item, access, capacity = lc.TryReceive()
	assert.Equal(t, nil, item)
	assert.False(t, access)
	assert.False(t, capacity)
}

// Ensure that Receive does not block when items are available
func TestReceiveWithItem(t *testing.T) {
	lc := New(0, 1)
	lc.TrySend(5)
	r := lc.Receive()
	assert.Equal(t, 5, r)
}

// Ensure that Receive does block when no items are available
func TestReceiveNoItem(t *testing.T) {
	lc := New(0, 1)
	assertBlocks(t, func() { lc.Receive() }, 10*time.Millisecond)
}

// Ensure that Send does not block when capacity is available
func TestSendWithCapacity(t *testing.T) {
	lc := New(0, 1)
	lc.Send(5)
}

// Ensure that Send does block when no capacity is available
func TestSendNoCapacity(t *testing.T) {
	lc := New(0, 1)
	lc.Send(5)
	assertBlocks(t, func() { lc.Send(7) }, 10*time.Millisecond)
}

// Ensure that items are received in the same order that they are sent with serial sends then receives
func TestFIFOOrderReceiveSerial(t *testing.T) {
	lc := New(0, len(smallItemSet))
	for _, i := range smallItemSet {
		lc.Send(i)
	}
	for _, i := range smallItemSet {
		r := lc.Receive()
		assert.Equal(t, i, r)
	}
}

// Ensure that items are received in the same order that they are sent with interleaved sends/receives
func TestFIFOOrderReceiveInterleaved(t *testing.T) {
	lc := New(0, 1)
	assert.Equal(t, 1, lc.Cap())
	for _, i := range smallItemSet {
		lc.Send(i)
		r := lc.Receive()
		assert.Equal(t, i, r)
	}
}

// Ensure thread safety in the case of M producers and N consumers
func TestMProducersNConsumers(t *testing.T) {
	// Number of elements to produce and consume in each configuration
	size := 1000

	// Try different numbers of producers and consumers
	for m := 1; m < 10; m++ {
		for n := 1; n < 10; n++ {
			// The LightChan used by the producers and consumers
			lc := New(0, size)

			// Source of the values that will be produced
			source := make(chan int, size)
			for i := 0; i < size; i++ {
				source <- i
			}

			// Destination for consumed values
			sink := make(chan int, size)

			// Start consumers
			for i := 0; i < n; i++ {
				go consume(lc, sink)
			}

			// Start producers
			for i := 0; i < m; i++ {
				go produce(lc, source)
			}

			// Record consumed values, looking for duplicates or not enough values consumed
			received := make([]bool, size)
			for i := 0; i < size; i++ {
				select {
				case next := <-sink:
					if !assert.False(t, received[next]) {
						t.FailNow()
					}
					received[next] = true
				case <-time.After(500 * time.Millisecond):
					assert.Fail(t, "Expected more values")
					t.FailNow()
				}
			}

			// Assert that all produced values were consumed
			for _, b := range received {
				// Check for missing value
				assert.True(t, b)
			}
		}
	}
}

// Keep receiving values from the LightChan and storing them in the sink
func consume(lc LightChan, sink chan<- int) {
	sugarChan := make(chan int)
	go func() {
		for {
			sugarChan <- lc.Receive().(int)
		}
	}()

ReceiveLoop:
	for {
		select {
		case i := <-sugarChan:
			sink <- i
		case <-time.After(100 * time.Millisecond):
			break ReceiveLoop
		}
	}
}

// Keep producing and sending values from the source to the LightChan until finished
func produce(lc LightChan, source <-chan int) {
SendLoop:
	for {
		select {
		case i := <-source:
			lc.Send(i)
		case <-time.After(100 * time.Millisecond):
			break SendLoop
		}
	}
}

// Expect a panic and recover
func assertPanic(t *testing.T) {
	assert.NotNil(t, recover())
}

// Assert that f blocks for at least duration
func assertBlocks(t *testing.T, f func(), duration time.Duration) {
	fDone := make(chan struct{})
	go func() {
		f()
		fDone <- struct{}{}
	}()

	select {
	case <-fDone:
		t.Fail()
	case <-time.After(duration):
		// Expected
	}
}

func BenchmarkGoChanSerial(b *testing.B) {
	theChan := make(chan int, 1)
	for n := 0; n < b.N; n++ {
		theChan <- 42
		<-theChan
	}
}

func BenchmarkLightChanSerial(b *testing.B) {
	theChan := New(0, 1)
	for n := 0; n < b.N; n++ {
		theChan.Send(42)
		theChan.Receive()
	}
}
