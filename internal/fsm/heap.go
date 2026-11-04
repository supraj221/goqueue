package fsm

// heapT is a minimal generic binary heap. Unlike container/heap it avoids
// interface boxing, and it reports each element's position through onIdx so
// callers can Fix or Remove an arbitrary element in O(log n).
type heapT[T any] struct {
	s     []T
	less  func(a, b T) bool
	onIdx func(x T, i int) // optional; called whenever an element moves
}

func (h *heapT[T]) len() int { return len(h.s) }

func (h *heapT[T]) peek() T { return h.s[0] }

func (h *heapT[T]) set(i int, x T) {
	h.s[i] = x
	if h.onIdx != nil {
		h.onIdx(x, i)
	}
}

func (h *heapT[T]) push(x T) {
	h.s = append(h.s, x)
	i := len(h.s) - 1
	if h.onIdx != nil {
		h.onIdx(x, i)
	}
	h.up(i)
}

func (h *heapT[T]) pop() T {
	return h.remove(0)
}

func (h *heapT[T]) remove(i int) T {
	last := len(h.s) - 1
	x := h.s[i]
	if i != last {
		h.set(i, h.s[last])
	}
	var zero T
	h.s[last] = zero
	h.s = h.s[:last]
	if i != last {
		if !h.down(i) {
			h.up(i)
		}
	}
	return x
}

func (h *heapT[T]) fix(i int) {
	if !h.down(i) {
		h.up(i)
	}
}

func (h *heapT[T]) up(i int) {
	x := h.s[i]
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(x, h.s[p]) {
			break
		}
		h.set(i, h.s[p])
		i = p
	}
	h.set(i, x)
}

func (h *heapT[T]) down(i0 int) bool {
	i := i0
	x := h.s[i]
	n := len(h.s)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		c := l
		if r := l + 1; r < n && h.less(h.s[r], h.s[l]) {
			c = r
		}
		if !h.less(h.s[c], x) {
			break
		}
		h.set(i, h.s[c])
		i = c
	}
	h.set(i, x)
	return i > i0
}
