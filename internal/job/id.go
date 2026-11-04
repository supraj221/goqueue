package job

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// crockford is the Crockford base32 alphabet; it sorts in ASCII order.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var idGen struct {
	sync.Mutex
	lastMs uint64
	seq    uint64 // random-seeded, incremented within the same millisecond
}

// NewID returns a 26-character ULID-style ID: 48 bits of Unix milliseconds
// followed by 80 bits that are random-seeded per millisecond and then
// incremented. IDs from one process are strictly increasing, so sorting by ID
// gives creation order.
func NewID(now time.Time) string {
	ms := uint64(now.UnixMilli())
	idGen.Lock()
	if ms <= idGen.lastMs {
		ms = idGen.lastMs
		idGen.seq++
	} else {
		idGen.lastMs = ms
		var b [8]byte
		_, _ = rand.Read(b[:])
		idGen.seq = binary.BigEndian.Uint64(b[:]) >> 1 // headroom before overflow
	}
	seq := idGen.seq
	idGen.Unlock()

	// 128 bits: 48 bits of time, then 16 zero bits, then the 64-bit sequence.
	var hi, lo uint64
	hi = ms << 16
	lo = seq
	var out [26]byte
	// Emit 26 base32 chars (130 bits; the top 2 bits are always zero).
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return string(out[:])
}
