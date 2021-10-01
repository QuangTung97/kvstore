package lease

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func assertEqualBytes(t *testing.T, expected, actual []byte) {
	t.Helper()
	assert.Equal(t, expected, actual)
}

func assertEqualGetStatus(t *testing.T, expected, actual GetStatus) {
	t.Helper()
	assert.Equal(t, expected, actual)
}

func TestCeilPowerOfTwo(t *testing.T) {
	result := ceilPowerOfTwo(100)
	assertEqualUint32(t, 128, result)

	result = ceilPowerOfTwo(16)
	assertEqualUint32(t, 16, result)

	result = ceilPowerOfTwo(255)
	assertEqualUint32(t, 256, result)

	result = ceilPowerOfTwo(0)
	assertEqualUint32(t, 1, result)
}

func TestCache_New(t *testing.T) {
	m := New(8, 1<<20, WithNumBuckets(120), WithLeaseListSize(5))
	if m.mask != 0x7f {
		t.Error("expected 0x7f, actual:", m.mask)
	}
	if len(m.leases) != 128 {
		t.Error("expected 128, actual:", len(m.leases))
	}
	if len(m.leases[0].list) != 8 {
		t.Error("expected 8, actual:", len(m.leases[0].list))
	}
}

func TestCache_Get_Rejected(t *testing.T) {
	m := New(8, 1<<20)
	key1 := []byte("key1")

	data := make([]byte, 1000)
	result := m.Get(key1, data)

	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	result = m.Get(key1, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseRejected, result.Status)

	assert.Equal(t, uint64(2), m.GetUnsafeInnerCache().GetAccessCount())
	assert.Equal(t, uint64(0), m.GetUnsafeInnerCache().GetHitCount())
}

func TestCache_Set_OK(t *testing.T) {
	m := New(8, 1<<20)
	key1 := []byte("key1")

	data := make([]byte, 1000)
	result := m.Get(key1, data)

	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assert.True(t, affected)

	result = m.Get(key1, data)
	assertEqualBytes(t, []byte("value1"), data[:result.ValueSize])
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusFound, result.Status)

	assert.Equal(t, uint64(2), m.GetUnsafeInnerCache().GetAccessCount())
	assert.Equal(t, uint64(1), m.GetUnsafeInnerCache().GetHitCount())
}

func TestCache_Set_Not_Affected_After_Invalidate(t *testing.T) {
	m := New(3, 1<<20)
	key1 := []byte("key1")

	data := make([]byte, 1000)
	result := m.Get(key1, data)

	affected := m.Invalidate(key1)
	assert.False(t, affected)

	affected = m.Set(key1, result.LeaseID, []byte("value1"))
	assert.False(t, affected)

	result = m.Get(key1, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestCache_Invalidate_Affected(t *testing.T) {
	m := New(4, 1<<20)
	key1 := []byte("key1")

	data := make([]byte, 1000)
	result := m.Get(key1, data)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assert.True(t, affected)

	affected = m.Invalidate(key1)
	assert.True(t, affected)

	result = m.Get(key1, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestCache_Double_Set_Not_OK(t *testing.T) {
	m := New(4, 1<<20)
	key1 := []byte("key1")

	data := make([]byte, 1000)
	result := m.Get(key1, data)

	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	affected := m.Set(key1, result.LeaseID, []byte("value1"))
	assert.True(t, affected)

	affected = m.Set(key1, result.LeaseID, []byte("value2"))
	assert.False(t, affected)

	result = m.Get(key1, data)
	assertEqualBytes(t, []byte("value1"), data[:result.ValueSize])
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusFound, result.Status)
}

func TestCache_Get_Second_Times_After_Lease_Timeout(t *testing.T) {
	m := New(8, 1<<20, WithLeaseTimeout(2))

	key := []byte("key")
	data := make([]byte, 1000)
	result := m.Get(key, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	time.Sleep(3 * time.Second)

	result = m.Get(key, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 2, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)
}

func TestCache_Get_Second_Times_Before_Lease_Timeout(t *testing.T) {
	m := New(8, 1<<20, WithLeaseTimeout(2))

	key := []byte("key")
	data := make([]byte, 1000)
	result := m.Get(key, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 1, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseGranted, result.Status)

	time.Sleep(1 * time.Second)

	result = m.Get(key, data)
	assert.Equal(t, 0, result.ValueSize)
	assertEqualUint32(t, 0, result.LeaseID)
	assertEqualGetStatus(t, GetStatusLeaseRejected, result.Status)
}

func TestComputeHashAndIndex(t *testing.T) {
	hash := uint64(0xaabbccdd11223344)
	key, index := computeHashKeyAndIndex(hash, 0xff)
	if key != 0xaabbccdd {
		t.Error("expected 0xaabbccdd, actual:", key)
	}
	if index != 0x44 {
		t.Error("expected 0x44, actual:", index)
	}
}

func BenchmarkGetSet(b *testing.B) {
	b.StopTimer()

	m := New(2, 128<<20)
	data := make([]byte, 1000)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprint("key-", i))
		result := m.Get(key, data)
		affected := m.Set(key, result.LeaseID, []byte("value"))
		if !affected {
			panic("not affected")
		}
	}
}

func BenchmarkParallelGetSet(b *testing.B) {
	b.StopTimer()

	m := New(64, 1<<20, WithNumBuckets(128), WithLeaseListSize(4))

	data := make([]byte, 1000)
	b.StartTimer()

	index := uint64(0)
	b.RunParallel(func(pb *testing.PB) {
		noopCount := 0

		for pb.Next() {
			i := atomic.AddUint64(&index, 1)

			key := []byte(fmt.Sprint("key-", i))
			result := m.Get(key, data)
			affected := m.Set(key, result.LeaseID, []byte("value"))
			if !affected {
				noopCount++
			}
		}

		fmt.Println(noopCount)
	})
}
