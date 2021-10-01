package lease

import (
	"github.com/QuangTung97/bigcache"
	"github.com/QuangTung97/bigcache/memhash"
	"math/bits"
)

// Cache ...
type Cache struct {
	leases []leaseList
	mask   uint32
	cache  *bigcache.Cache
}

// New ...
func New(numSegments int, segmentSize int, options ...Option) *Cache {
	opts := computeOptions(options...)

	leases := make([]leaseList, opts.numBuckets)
	for i := range leases {
		leases[i].init(opts.entryListSize, opts.leaseTimeout)
	}

	return &Cache{
		leases: leases,
		cache:  bigcache.New(numSegments, segmentSize),
		mask:   opts.numBuckets - 1,
	}
}

func hashFunc(data []byte) uint64 {
	return memhash.Hash(data)
}

func getNow() uint32 {
	return uint32(memhash.NanoTime() / 1000000000)
}

// GetStatus for cache Get
type GetStatus int

const (
	// GetStatusFound for normal cache hit case
	GetStatusFound GetStatus = iota
	// GetStatusLeaseGranted when cache miss and lease is granted
	GetStatusLeaseGranted
	// GetStatusLeaseRejected when cache miss and lease is not granted
	GetStatusLeaseRejected
)

// GetResult for result when calling Get
type GetResult struct {
	Status    GetStatus
	LeaseID   uint32
	ValueSize int
}

func computeHashKeyAndIndex(hash uint64, mask uint32) (hashKey uint32, index uint32) {
	return uint32(hash >> 32), uint32(hash) & mask
}

func (c *Cache) getLeaseList(key []byte) (uint32, *leaseList) {
	hash := hashFunc(key)
	hashKey, index := computeHashKeyAndIndex(hash, c.mask)
	return hashKey, &c.leases[index]
}

// Get value from the cache
func (c *Cache) Get(key []byte, value []byte) GetResult {
	size, ok := c.cache.Get(key, value)
	if ok {
		return GetResult{
			Status:    GetStatusFound,
			ValueSize: size,
		}
	}

	hashKey, l := c.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	leaseID, ok := l.getLease(hashKey, getNow())
	if !ok {
		return GetResult{
			Status: GetStatusLeaseRejected,
		}
	}

	return GetResult{
		LeaseID: leaseID,
		Status:  GetStatusLeaseGranted,
	}
}

// Set value to the cache
func (c *Cache) Set(key []byte, leaseID uint32, value []byte) (affected bool) {
	hashKey, l := c.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	deleted := l.deleteLease(hashKey, leaseID)
	if !deleted {
		return false
	}

	c.cache.Put(key, value)
	return true
}

// Invalidate an entry from the cache
func (c *Cache) Invalidate(key []byte) (affected bool) {
	hashKey, l := c.getLeaseList(key)

	l.mut.Lock()
	defer l.mut.Unlock()

	l.forceDelete(hashKey)

	return c.cache.Delete(key)
}

// GetUnsafeInnerCache returns the bigcache
func (c *Cache) GetUnsafeInnerCache() *bigcache.Cache {
	return c.cache
}

func ceilPowerOfTwo(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	shift := 32 - bits.LeadingZeros32(n-1)
	return 1 << shift
}
