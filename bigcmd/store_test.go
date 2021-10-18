package bigcmd

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func newStore(size int) *Store {
	s := &Store{}
	InitStore(s, size, 1000)
	return s
}

func TestStore_Normal(t *testing.T) {
	s := newStore(28)
	data := []byte(strings.Repeat("A", 10))

	filled := s.Put(10, 20, 0, data)
	assert.Equal(t, false, filled)

	filled = s.Put(10, 20, 10, data)
	assert.Equal(t, true, filled)

	result := s.Get(10)
	assert.Equal(t, []byte(strings.Repeat("A", 20)), result)
}

func TestStore_Data_Bigger_Than_Length(t *testing.T) {
	s := newStore(28)

	data := []byte(strings.Repeat("A", 10))
	filled := s.Put(10, 20, 0, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("A", 11))
	filled = s.Put(10, 20, 10, data)
	assert.Equal(t, false, filled)

	result := s.Get(10)
	assert.Equal(t, []byte(nil), result)
}

func TestStore_Put_Asymmetric(t *testing.T) {
	s := newStore(28)

	data := []byte(strings.Repeat("A", 8))
	filled := s.Put(10, 19, 0, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("B", 11))
	filled = s.Put(10, 19, 8, data)
	assert.Equal(t, true, filled)

	data = s.Get(10)
	assert.Equal(t, []byte(strings.Repeat("A", 8)+strings.Repeat("B", 11)), data)
}

func TestStore_Remove_Least_Recent(t *testing.T) {
	s := newStore(28)

	data := []byte(strings.Repeat("A", 8))
	filled := s.Put(10, 19, 0, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("A", 10))
	filled = s.Put(11, 20, 10, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("B", 11))
	filled = s.Put(10, 19, 8, data)
	assert.Equal(t, false, filled)
}

func TestStore_Put_Wrap_Around(t *testing.T) {
	s := newStore(28)

	data := []byte(strings.Repeat("A", 8))
	filled := s.Put(50, 19, 0, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("C", 7))
	filled = s.Put(51, 13, 0, data)
	assert.Equal(t, false, filled)

	data = []byte(strings.Repeat("D", 6))
	filled = s.Put(51, 13, 7, data)
	assert.Equal(t, true, filled)

	data = s.Get(51)
	assert.Equal(t, []byte(strings.Repeat("C", 7)+strings.Repeat("D", 6)), data)
}
