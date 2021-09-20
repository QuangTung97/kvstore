package kvstore

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	server := NewServer()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := server.Run()
		fmt.Println("RUN:", err)
	}()

	time.Sleep(10 * time.Millisecond)

	client, err := NewClient("localhost:7000")
	assert.Equal(t, nil, err)

	ctx := context.Background()
	err = client.Pipelined(ctx, func(p *Pipeline) error {
		p.DoSomething()
		return nil
	})
	assert.Equal(t, nil, err)

	time.Sleep(10 * time.Millisecond)

	client.Shutdown()
}
