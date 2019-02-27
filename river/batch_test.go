package river

import (
	"reflect"
	"testing"
	"time"

	"github.com/siddontang/go-mysql/canal"
)

func TestBatchedChangeSet_AddTableRowChange(t *testing.T) {
	t1 := time.Date(2017, 12, 10, 21, 51, 47, 3485, time.UTC)
	t2 := time.Date(2017, 12, 10, 20, 11, 25, 43958, time.UTC)
	c1 := TableRowChange{
		Action: canal.InsertAction,
		Index:  "i1",
		DocID:  112233,
		TS:     t2,
	}
	c2 := TableRowChange{
		Action: canal.InsertAction,
		Index:  "i1",
		DocID:  112234,
		TS:     t1,
	}
	c3 := TableRowChange{
		Action: canal.DeleteAction,
		Index:  "i1",
		DocID:  112233,
		TS:     t1,
	}
	c4 := TableRowChange{
		Action: canal.UpdateAction,
		Index:  "i1",
		DocID:  112235,
		TS:     t1,
	}
	c5 := TableRowChange{
		Action: canal.UpdateAction,
		Index:  "i2",
		DocID:  112233,
		TS:     t2,
	}
	c6 := TableRowChange{
		Action: canal.UpdateAction,
		Index:  "i2",
		DocID:  112234,
		TS:     t2,
	}
	type fields struct {
		changes map[string]IndexChangeSet
		label   string
	}
	tests := []struct {
		name         string
		initialState map[string]IndexChangeSet
		rowChanges   []TableRowChange
		want         map[string]IndexChangeSet
	}{
		{name: "one index one document one change",
			initialState: map[string]IndexChangeSet{},
			rowChanges:   []TableRowChange{c1},
			want: map[string]IndexChangeSet{
				"i1": {
					Index: "i1",
					ChangedDocs: map[uint64]DocumentChange{
						112233: {
							Changes: []TableRowChange{c1},
							DocID:   112233,
							FirstTS: t2,
							LastTS:  t2,
							Index:   "i1"}}}}},
		{name: "one index one document multiple changes",
			initialState: map[string]IndexChangeSet{},
			rowChanges:   []TableRowChange{c1, c3},
			want: map[string]IndexChangeSet{
				"i1": {
					Index: "i1",
					ChangedDocs: map[uint64]DocumentChange{
						112233: {
							Changes: []TableRowChange{c1, c3},
							DocID:   112233,
							FirstTS: t2,
							LastTS:  t1,
							Index:   "i1"}}}}},
		{name: "one index multiple documents multiple changes",
			initialState: map[string]IndexChangeSet{},
			rowChanges:   []TableRowChange{c1, c2, c3, c4},
			want: map[string]IndexChangeSet{
				"i1": {
					Index: "i1",
					ChangedDocs: map[uint64]DocumentChange{
						112233: {
							Changes: []TableRowChange{c1, c3},
							DocID:   112233,
							FirstTS: t2,
							LastTS:  t1,
							Index:   "i1"},
						112234: {
							Changes: []TableRowChange{c2},
							DocID:   112234,
							FirstTS: t1,
							LastTS:  t1,
							Index:   "i1"},
						112235: {
							Changes: []TableRowChange{c4},
							DocID:   112235,
							FirstTS: t1,
							LastTS:  t1,
							Index:   "i1"}}}}},
		{name: "multiple indices multiple documents multiple changes",
			initialState: map[string]IndexChangeSet{},
			rowChanges:   []TableRowChange{c1, c5, c6, c2, c3, c4, c5, c3},
			want: map[string]IndexChangeSet{
				"i1": {
					Index: "i1",
					ChangedDocs: map[uint64]DocumentChange{
						112233: {
							Changes: []TableRowChange{c1, c3, c3},
							DocID:   112233,
							FirstTS: t2,
							LastTS:  t1,
							Index:   "i1"},
						112234: {
							Changes: []TableRowChange{c2},
							DocID:   112234,
							FirstTS: t1,
							LastTS:  t1,
							Index:   "i1"},
						112235: {
							Changes: []TableRowChange{c4},
							DocID:   112235,
							FirstTS: t1,
							LastTS:  t1,
							Index:   "i1"}}},
				"i2": {
					Index: "i2",
					ChangedDocs: map[uint64]DocumentChange{
						112233: {
							Changes: []TableRowChange{c5, c5},
							DocID:   112233,
							FirstTS: t2,
							LastTS:  t2,
							Index:   "i2"},
						112234: {
							Changes: []TableRowChange{c6},
							DocID:   112234,
							FirstTS: t2,
							LastTS:  t2,
							Index:   "i2"}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BatchedChangeSet{
				Changes: tt.initialState,
				Label:   "test",
			}
			for _, c := range tt.rowChanges {
				b.AddRowChange(c)
			}
			got := b.Changes
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("changes after AddTableRowChange(): %v, want %v", got, tt.want)
			}
		})
	}
}
