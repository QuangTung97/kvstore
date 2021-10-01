package lease

type cacheOptions struct {
	numBuckets    uint32
	entryListSize uint32
	leaseTimeout  uint32
}

// Option ...
type Option func(opts *cacheOptions)

func computeOptions(options ...Option) cacheOptions {
	result := cacheOptions{
		numBuckets:    1024,
		entryListSize: 16,
		leaseTimeout:  30,
	}

	for _, o := range options {
		o(&result)
	}
	return result
}

// WithNumBuckets configures number of lease buckets
func WithNumBuckets(n uint32) Option {
	return func(opts *cacheOptions) {
		opts.numBuckets = ceilPowerOfTwo(n)
	}
}

// WithLeaseListSize configures the number of entries in a lease list
func WithLeaseListSize(n uint32) Option {
	return func(opts *cacheOptions) {
		opts.entryListSize = ceilPowerOfTwo(n)
	}
}

// WithLeaseTimeout for duration of lease timeout, in second
func WithLeaseTimeout(d uint32) Option {
	return func(opts *cacheOptions) {
		opts.leaseTimeout = d
	}
}
