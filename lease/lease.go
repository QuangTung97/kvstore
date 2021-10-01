package lease

import "sync"

type leaseEntry struct {
	hash      uint32
	lease     uint32
	createdAt uint32
}

type leaseList struct {
	mut       sync.Mutex
	list      []leaseEntry
	expire    uint32
	nextLease uint32
	_padding  [3]uint64 // to eliminate cache line false sharing
}

func (l *leaseList) init(size uint32, expire uint32) {
	l.list = make([]leaseEntry, size)
	l.expire = expire
}

func (l *leaseList) increase() {
	l.nextLease++
	if l.nextLease == 0 {
		l.nextLease = 1
	}
}

func (l *leaseList) getLease(hash uint32, now uint32) (uint32, bool) {
	for i, e := range l.list {
		if e.createdAt+l.expire <= now {
			l.list[i] = leaseEntry{}
		}
	}

	minLease := l.list[0].lease
	minIndex := 0
	for i, e := range l.list {
		if e.hash == hash && e.lease > 0 {
			return 0, false
		}

		if e.lease < minLease {
			minLease = e.lease
			minIndex = i
		}
	}

	l.increase()
	l.list[minIndex] = leaseEntry{
		hash:      hash,
		lease:     l.nextLease,
		createdAt: now,
	}

	return l.nextLease, true
}

func (l *leaseList) deleteLease(hash uint32, lease uint32) bool {
	for i, e := range l.list {
		if e.hash == hash && e.lease == lease {
			l.list[i] = leaseEntry{}
			return true
		}
	}
	return false
}

func (l *leaseList) forceDelete(hash uint32) {
	for i, e := range l.list {
		if e.hash == hash {
			l.list[i] = leaseEntry{}
		}
	}
}
