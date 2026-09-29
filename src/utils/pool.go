package utils

import "sync"

type BufferPool[T any] struct {
	pool sync.Pool
}

func NewBufferPool[T any]() *BufferPool[T] {
	return &BufferPool[T]{
		pool: sync.Pool{
			New: func() any {
				b := make([]T, 0, 128)
				return &b
			},
		},
	}
}

func (p *BufferPool[T]) Get() *[]T {
	buf := p.pool.Get().(*[]T)
	*buf = (*buf)[:0]
	return buf
}

func (p *BufferPool[T]) Put(buf *[]T) {
	if buf == nil {
		return
	}
	if cap(*buf) > 10000 {
		return
	}
	clear(*buf)
	*buf = (*buf)[:0]
	p.pool.Put(buf)
}

type MapPool[K comparable, V any] struct {
	pool sync.Pool
}

func NewMapPool[K comparable, V any]() *MapPool[K, V] {
	return &MapPool[K, V]{
		pool: sync.Pool{
			New: func() any {
				return make(map[K]V, 128)
			},
		},
	}
}

func (p *MapPool[K, V]) Get() map[K]V {
	return p.pool.Get().(map[K]V)
}

func (p *MapPool[K, V]) Put(m map[K]V) {
	if m == nil {
		return
	}
	if len(m) > 10000 {
		return
	}
	clear(m)
	p.pool.Put(m)
}
