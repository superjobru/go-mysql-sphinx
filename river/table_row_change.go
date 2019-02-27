package river

import (
	"reflect"
	"time"
)

// PrimaryKey value of a primary key in a given table
// we need this to figure out which rows were inserted/deleted/updated
type PrimaryKey struct {
	Len   int
	Value [16]interface{}
}

// TableRowChange represents a change that may lead to document update in sphinx index
type TableRowChange struct {
	Action    string
	Columns   []string
	ColumnMap map[string][]string
	DocID     uint64
	EventID   uint64
	Index     string
	OldRow    []interface{}
	NewRow    []interface{}
	PK        PrimaryKey
	TableName string
	TS        time.Time
}

// ValuePair instead of a tuple
type ValuePair struct {
	Old interface{}
	New interface{}
}

// ColumnDiff just a helper to avoid lengthy expressions
func (c TableRowChange) ColumnDiff(colNo int) ValuePair {
	return ValuePair{
		Old: hashable(c.OldRow[colNo]),
		New: hashable(c.NewRow[colNo]),
	}
}

// DocIDList returns list if document IDs for a slice of table row changes
// used for logging
func DocIDList(c []TableRowChange) []uint64 {
	list := make([]uint64, len(c))
	for i, r := range c {
		list[i] = r.DocID
	}
	return list
}

// HasSameValues are the values same?
func (v ValuePair) HasSameValues() bool {
	return reflect.DeepEqual(v.Old, v.New)
}

func hashable(value interface{}) interface{} {
	bytes, isByteSlice := value.([]uint8)
	if isByteSlice {
		return string(bytes)
	}
	return value
}
