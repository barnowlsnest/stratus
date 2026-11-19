package stream

import (
	"errors"

	"github.com/barnowlsnest/stratus/internal/storage"
)

// recordOverhead is an upper bound on what protobuf adds to each record in a
// ReadResponse: the record's field tag and length, the ID's tag and varint, and
// the payload's tag and length; about 11 bytes for typical IDs and payloads.
// Counting it keeps a message of many small records within the budget too.
const recordOverhead = 20

// errBudgetSpent stops a WAL read once the read's byte budget refuses a record.
// It never leaves the package.
var errBudgetSpent = errors.New("stream: read budget spent")

// readBudget counts the encoded size of one read, each record's payload plus
// recordOverhead, against a limit. It always takes the first record, however
// large, so a reader never stalls on one record bigger than the limit. A zero
// limit takes every record.
type readBudget struct {
	limit int
	used  int
	taken int
}

// take reports whether a record with a payload of size bytes still fits, and
// counts it if so.
func (b *readBudget) take(size int) bool {
	size += recordOverhead
	if b.limit > 0 && b.taken > 0 && b.used+size > b.limit {
		return false
	}

	b.used += size
	b.taken++

	return true
}

// fit takes records from index from on and cuts records before the first one
// the budget refuses. It reports whether every record fit. The cut-off entries
// are cleared, so the reused slice does not keep their records alive.
func (b *readBudget) fit(records []*storage.Record, from int) ([]*storage.Record, bool) {
	if b.limit <= 0 {
		return records, true
	}

	for i := from; i < len(records); i++ {
		if !b.take(len(records[i].Bytes)) {
			clear(records[i:])

			return records[:i], false
		}
	}

	return records, true
}
