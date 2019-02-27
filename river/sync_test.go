package river

import (
	"reflect"
	"testing"
	"time"
)

func Test_syncState_SplitQueue(t *testing.T) {
	threshold := time.Date(2018, 07, 11, 18, 55, 20, 0, time.UTC)
	tsToStage := time.Date(2018, 07, 11, 18, 55, 10, 0, time.UTC)
	tsToNotStage := time.Date(2018, 07, 11, 18, 55, 30, 0, time.UTC)
	isReady := func(c DocumentChange) bool {
		return c.LastTS.Before(threshold)
	}
	tests := []struct {
		name  string
		state syncState
		want  syncState
	}{
		{name: "no changes yet",
			state: syncState{
				queue:  BatchedChangeSet{Label: "queue", Changes: map[string]IndexChangeSet{}},
				staged: BatchedChangeSet{Label: "staged", Changes: map[string]IndexChangeSet{}}},
			want: syncState{
				queue:  BatchedChangeSet{Label: "queue", Changes: map[string]IndexChangeSet{}},
				staged: BatchedChangeSet{Label: "staged", Changes: map[string]IndexChangeSet{}}}},
		{name: "stages changes should be discarded",
			state: syncState{
				queue: BatchedChangeSet{Label: "queue", Changes: map[string]IndexChangeSet{}},
				staged: BatchedChangeSet{Label: "staged",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								111: {LastTS: tsToStage}}}}}},
			want: syncState{
				queue:  BatchedChangeSet{Label: "queue", Changes: map[string]IndexChangeSet{}},
				staged: BatchedChangeSet{Label: "staged", Changes: map[string]IndexChangeSet{}}}},
		{name: "changes for single index should be split properly",
			state: syncState{
				queue: BatchedChangeSet{Label: "queue",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								111: {Index: "index1", DocID: 111, LastTS: tsToNotStage},
								222: {Index: "index1", DocID: 222, LastTS: tsToStage},
								333: {Index: "index1", DocID: 333, LastTS: tsToNotStage}}}}},
				staged: BatchedChangeSet{Label: "staged", Changes: map[string]IndexChangeSet{}}},
			want: syncState{
				queue: BatchedChangeSet{Label: "queue",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								111: {Index: "index1", DocID: 111, LastTS: tsToNotStage},
								333: {Index: "index1", DocID: 333, LastTS: tsToNotStage}}}}},
				staged: BatchedChangeSet{Label: "staged",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								222: {Index: "index1", DocID: 222, LastTS: tsToStage}}}}}}},
		{name: "changes for multiple indexes should be split properly",
			state: syncState{
				queue: BatchedChangeSet{Label: "queue",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								222: {Index: "index1", DocID: 222, LastTS: tsToStage},
								333: {Index: "index1", DocID: 333, LastTS: tsToNotStage}}},
						"index2": IndexChangeSet{Index: "index2",
							ChangedDocs: map[uint64]DocumentChange{
								222: {Index: "index2", DocID: 222, LastTS: tsToNotStage},
								333: {Index: "index2", DocID: 333, LastTS: tsToNotStage}}},
						"index3": IndexChangeSet{Index: "index3",
							ChangedDocs: map[uint64]DocumentChange{
								444: {Index: "index3", DocID: 444, LastTS: tsToStage},
								555: {Index: "index3", DocID: 555, LastTS: tsToStage},
								666: {Index: "index3", DocID: 666, LastTS: tsToStage}}}}},
				staged: BatchedChangeSet{Label: "staged", Changes: map[string]IndexChangeSet{}}},
			want: syncState{
				queue: BatchedChangeSet{Label: "queue",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								333: {Index: "index1", DocID: 333, LastTS: tsToNotStage}}},
						"index2": IndexChangeSet{Index: "index2",
							ChangedDocs: map[uint64]DocumentChange{
								222: {Index: "index2", DocID: 222, LastTS: tsToNotStage},
								333: {Index: "index2", DocID: 333, LastTS: tsToNotStage}}}}},
				staged: BatchedChangeSet{Label: "staged",
					Changes: map[string]IndexChangeSet{
						"index1": IndexChangeSet{Index: "index1",
							ChangedDocs: map[uint64]DocumentChange{
								222: {Index: "index1", DocID: 222, LastTS: tsToStage}}},
						"index3": IndexChangeSet{Index: "index3",
							ChangedDocs: map[uint64]DocumentChange{
								444: {Index: "index3", DocID: 444, LastTS: tsToStage},
								555: {Index: "index3", DocID: 555, LastTS: tsToStage},
								666: {Index: "index3", DocID: 666, LastTS: tsToStage}}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.state
			s.SplitQueue(isReady)
			if !reflect.DeepEqual(s, tt.want) {
				t.Errorf("after SplitQueue(): state = %v, want %v", s, tt.want)
			}
		})
	}
}
