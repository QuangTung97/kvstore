package kvstore

import (
	"github.com/QuangTung97/kvstore/bigcmd"
	"github.com/QuangTung97/kvstore/lease"
	"sync"
)

type receiver struct {
	processors []*processor
	store      bigcmd.Store
	sequence   uint64 // for selecting next processor
	wg         sync.WaitGroup
}

func initReceiver(
	r *receiver, cache *lease.Cache,
	sender ResponseSender, options kvstoreOptions,
) {
	processors := make([]*processor, 0, options.numProcessors)
	for i := 0; i < options.numProcessors; i++ {
		processors = append(processors, newProcessor(cache, sender, options))
	}
	r.processors = processors
	r.sequence = 0

	bigcmd.InitStore(&r.store, options.bigCommandStoreSize, options.maxBatchSize)
}

func (r *receiver) recv(ip IPAddr, port uint16, data []byte) {
	header, nextOffset := parseDataFrameHeader(data)
	data = data[nextOffset:]

	if header.fragmented {
		filled := r.store.Put(header.batchID, header.length, header.offset, data)
		if !filled {
			return
		}
		data = r.store.Get(header.batchID)
	}

	for {
		seq := r.sequence
		r.sequence++
		index := seq % uint64(len(r.processors))
		p := r.processors[index]
		if !p.isCommandAppendable(len(data)) {
			continue
		}
		p.appendCommands(ip, port, data)
		return
	}
}

func (r *receiver) runInBackground() {
	r.wg.Add(len(r.processors))
	for _, p := range r.processors {
		proc := p
		go func() {
			defer r.wg.Done()
			proc.run()
		}()
	}
}

func (r *receiver) shutdown() {
	for _, p := range r.processors {
		p.shutdown()
	}
	r.wg.Wait()
}
