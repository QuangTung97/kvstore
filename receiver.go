package kvstore

import (
	"github.com/QuangTung97/kvstore/lease"
	"sync"
)

type receiver struct {
	processors []*processor
	sequence   atomicUint64 // for selecting next processor
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
	r.sequence.value = 0
}

func (r *receiver) recv(ip IPAddr, port uint16, data []byte) {
	for {
		_, nextOffset := parseDataFrameHeader(data)
		data = data[nextOffset:]

		seq := r.sequence.increase()
		index := seq % uint64(len(r.processors))
		p := r.processors[index]
		if !p.isCommandAppendable(len(data)) {
			continue
		}
		p.appendCommands(ip, port, data)
		break
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
