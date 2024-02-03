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

package rangelock_test

import (
	"context"
	"testing"

	"github.com/bucket-sailor/rangelock"
	"github.com/stretchr/testify/require"
)

func TestRangeLock(t *testing.T) {
	rl := rangelock.New()

	ctx := context.Background()
	id, err := rl.Lock(ctx, 0, 100)
	require.NoError(t, err)

	_, ok := rl.TryLock(1, 101)
	require.False(t, ok, "tryLock succeeded with mutex locked")

	_, ok = rl.TryRLock(2, 102)
	require.False(t, ok, "tryRLock succeeded with mutex locked")

	rl.Unlock(id)

	id, ok = rl.TryLock(0, 100)
	require.True(t, ok, "tryLock failed with mutex unlocked")

	rl.Unlock(id)

	id, ok = rl.TryRLock(0, 100)
	require.True(t, ok, "tryRLock failed with mutex unlocked")

	id2, ok := rl.TryRLock(1, 101)
	require.True(t, ok, "tryRLock failed with mutex unlocked")

	_, ok = rl.TryLock(2, 102)
	require.False(t, ok, "tryLock succeeded with mutex locked")

	rl.Unlock(id)
	rl.Unlock(id2)

	id, ok = rl.TryLock(2, 102)
	require.True(t, ok, "tryLock failed with mutex unlocked")

	rl.Unlock(id)
}
