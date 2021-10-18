package kvstore

import "go.uber.org/zap"

type kvstoreOptions struct {
	numProcessors        int
	bufferSize           int
	maxResultPackageSize int

	bigCommandStoreSize int
	maxBatchSize        int

	logger *zap.Logger
}

// Option ...
type Option func(opts *kvstoreOptions)

func computeOptions(options ...Option) kvstoreOptions {
	opts := kvstoreOptions{
		numProcessors:        4,
		bufferSize:           2 << 20, // 2MB
		maxResultPackageSize: 1 << 15, // 32KB

		bigCommandStoreSize: 8 << 20, // 8MB
		maxBatchSize:        1 << 20, // 1MB

		logger: zap.NewNop(),
	}
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// WithNumProcessors ...
func WithNumProcessors(n int) Option {
	return func(opts *kvstoreOptions) {
		opts.numProcessors = n
	}
}

// WithBufferSize ...
func WithBufferSize(size int) Option {
	return func(opts *kvstoreOptions) {
		opts.bufferSize = size
	}
}

// WithMaxResultPackageSize ...
func WithMaxResultPackageSize(size int) Option {
	return func(opts *kvstoreOptions) {
		opts.maxResultPackageSize = size
	}
}

// WithMaxBatchSize ...
func WithMaxBatchSize(size int) Option {
	return func(opts *kvstoreOptions) {
		opts.maxBatchSize = size
	}
}

// WithLogger ...
func WithLogger(logger *zap.Logger) Option {
	return func(opts *kvstoreOptions) {
		opts.logger = logger
	}
}
