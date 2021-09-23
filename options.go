package kvstore

import "go.uber.org/zap"

type kvstoreOptions struct {
	maxResultPackageSize int
	logger               *zap.Logger
}

// Option ...
type Option func(opts *kvstoreOptions)

func computeOptions(options ...Option) kvstoreOptions {
	opts := kvstoreOptions{
		maxResultPackageSize: 1 << 15,
		logger:               zap.NewNop(),
	}
	for _, o := range options {
		o(&opts)
	}
	return opts
}

// WithMaxResultPackageSize ...
func WithMaxResultPackageSize(size int) Option {
	return func(opts *kvstoreOptions) {
		opts.maxResultPackageSize = size
	}
}

// WithLogger ...
func WithLogger(logger *zap.Logger) Option {
	return func(opts *kvstoreOptions) {
		opts.logger = logger
	}
}
