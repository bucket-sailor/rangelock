/* SPDX-License-Identifier: Apache-2.0
 *
 * Copyright (c) 2024 Damian Peckett <damian@pecke.tt>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rangelock

import (
	"context"
	"sync"

	"github.com/Workiva/go-datastructures/augmentedtree"
)

type Range struct {
	id        uint64
	low, high int64
	write     bool
}

func (r Range) ID() uint64                     { return r.id }
func (r Range) LowAtDimension(_ uint64) int64  { return r.low }
func (r Range) HighAtDimension(_ uint64) int64 { return r.high }
func (r Range) OverlapsAtDimension(interval augmentedtree.Interval, dim uint64) bool {
	return r.low <= interval.HighAtDimension(dim) && r.high >= interval.LowAtDimension(dim)
}

// RangeLock is a effectively a sync.RWMutex that allows locking of ranges of
// integers. It is safe for concurrent use by multiple goroutines.
type RangeLock struct {
	mu        sync.RWMutex
	cond      *sync.Cond
	tree      augmentedtree.Tree
	idCounter uint64
}

func New() *RangeLock {
	rl := &RangeLock{
		tree: augmentedtree.New(1),
	}
	rl.cond = sync.NewCond(&rl.mu)
	return rl
}

// Lock locks the range for writing.
func (rl *RangeLock) Lock(ctx context.Context, low, high int64) (uint64, error) {
	for {
		id, locked := rl.TryLock(low, high)
		if locked {
			return id, nil
		}

		waitCh := make(chan struct{})
		go func() {
			rl.cond.Wait()
			close(waitCh)
		}()

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-waitCh:
		}
	}
}

// TryLock tries to lock the range for writing. If the lock is already held by
// another writer or a reader, it returns false.
func (rl *RangeLock) TryLock(low, high int64) (uint64, bool) {
	if rl.checkOverlap(Range{low: low, high: high, write: true}) {
		return 0, false
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	r := Range{
		low:   low,
		high:  high,
		id:    rl.nextID(),
		write: true,
	}
	rl.tree.Add(r)
	return r.ID(), true
}

// RLock locks the range for reading.
func (rl *RangeLock) RLock(ctx context.Context, low, high int64) (uint64, error) {
	for {
		id, locked := rl.TryRLock(low, high)
		if locked {
			return id, nil
		}

		waitCh := make(chan struct{})
		go func() {
			rl.cond.Wait()
			close(waitCh)
		}()

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-waitCh:
		}
	}
}

// TryRLock tries to lock the range for reading. If the lock is already held by
// a writer, it returns false.
func (rl *RangeLock) TryRLock(low, high int64) (uint64, bool) {
	if rl.checkOverlap(Range{low: low, high: high}) {
		return 0, false
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	r := Range{
		low:  low,
		high: high,
		id:   rl.nextID(),
	}
	rl.tree.Add(r)
	return r.ID(), true
}

// Unlock unlocks the lock with the given identifier.
func (rl *RangeLock) Unlock(id uint64) {
	rl.mu.Lock()
	rl.tree.Delete(Range{id: id})
	rl.mu.Unlock()

	rl.cond.Broadcast()
}

func (rl *RangeLock) checkOverlap(query Range) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	overlaps := rl.tree.Query(query)
	for _, interval := range overlaps {
		other := interval.(Range)
		if query.write || other.write {
			return true
		}
	}
	return false
}

func (rl *RangeLock) nextID() uint64 {
	rl.idCounter++
	return rl.idCounter
}
