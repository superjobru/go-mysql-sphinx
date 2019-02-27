package river

import (
	"reflect"
	"testing"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

func TestIngestRule_Apply(t *testing.T) {
	testTime := time.Date(2018, 7, 11, 5, 44, 20, 483925, time.UTC)
	testTimeProvider := func() time.Time { return testTime }
	cm := map[string][]string{"col1": {}}
	r := IngestRule{
		TableName:    "db1.t1",
		DocIDField:   "id",
		Index:        "i1",
		ColumnMap:    cm,
		timeProvider: testTimeProvider,
	}
	row1 := []interface{}{1, 31337, "javascript"}
	row2 := []interface{}{2, 13373, "closurescript"}
	row3 := []interface{}{3, 33731, "typescript"}
	row1mod := []interface{}{1, 137, "javascript"}
	row2mod := []interface{}{2, 317, "purescript"}
	row3mod := []interface{}{3, 713, "coffeescript"}
	type args struct {
		e *canal.RowsEvent
	}
	tests := []struct {
		name    string
		args    args
		want    []TableRowChange
		wantErr bool
	}{
		{name: "insert to unrelated table",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "db2",
						Name:   "t2",
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"},
						},
					},
					Action: canal.InsertAction,
					Rows:   [][]interface{}{row1},
					Header: &replication.EventHeader{}}},
			want:    nil,
			wantErr: false},
		{name: "update to unrelated table",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "db2",
						Name:   "t2",
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.UpdateAction,
					Rows:   [][]interface{}{row1, row2},
					Header: &replication.EventHeader{}}},
			want:    nil,
			wantErr: false},
		{name: "delete from unrelated table",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema: "db2",
						Name:   "t2",
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.DeleteAction,
					Rows:   [][]interface{}{row1, row2},
					Header: &replication.EventHeader{}}},
			want:    nil,
			wantErr: false},
		{name: "insert single row",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.InsertAction,
					Rows:   [][]interface{}{row1},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{31337},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false,
		},
		{name: "insert single row with no primary key",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.InsertAction,
					Rows:   [][]interface{}{row1},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{31337},
					PK:        newPrimaryKey([]interface{}{1, 31337, "javascript"}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false,
		},
		{name: "insert single row with a byte slice",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.InsertAction,
					Rows:   [][]interface{}{{1, 31337, []uint8("javascript")}},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{31337},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false,
		},
		{name: "insert multiple rows",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"},
						},
					},
					Action: canal.InsertAction,
					Rows:   [][]interface{}{row1, row2, row3},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{31337},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     2,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{13373},
					PK:        newPrimaryKey([]interface{}{2}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					Index:     "i1",
					DocID:     3,
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{33731},
					PK:        newPrimaryKey([]interface{}{3}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false},
		{name: "update single row without id change",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.UpdateAction,
					Rows:   [][]interface{}{row1, row1mod},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.UpdateAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{31337},
					NewRow:    []interface{}{137},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false},
		{name: "update single row with id change",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{2},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.UpdateAction,
					Rows:   [][]interface{}{row1, row2},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.DeleteAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{31337},
					NewRow:    []interface{}{nil},
					PK:        newPrimaryKey([]interface{}{"javascript"}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     2,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{13373},
					PK:        newPrimaryKey([]interface{}{"closurescript"}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false},
		{name: "update multiple rows without id change",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.UpdateAction,
					Rows: [][]interface{}{
						row1, row1mod,
						row2, row2mod,
						row3, row3mod},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.UpdateAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{31337},
					NewRow:    []interface{}{137},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.UpdateAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     2,
					Index:     "i1",
					OldRow:    []interface{}{13373},
					NewRow:    []interface{}{317},
					PK:        newPrimaryKey([]interface{}{2}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.UpdateAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     3,
					Index:     "i1",
					OldRow:    []interface{}{33731},
					NewRow:    []interface{}{713},
					PK:        newPrimaryKey([]interface{}{3}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false},
		{name: "update multiple rows with id change",
			args: args{
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "col1"},
							{Name: "text1"}}},
					Action: canal.UpdateAction,
					Rows: [][]interface{}{
						row1, row1mod,
						row2, row3},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.UpdateAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     1,
					Index:     "i1",
					OldRow:    []interface{}{31337},
					NewRow:    []interface{}{137},
					PK:        newPrimaryKey([]interface{}{1}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.DeleteAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     2,
					Index:     "i1",
					OldRow:    []interface{}{13373},
					NewRow:    []interface{}{nil},
					PK:        newPrimaryKey([]interface{}{2}),
					TableName: "db1.t1",
					TS:        testTime},
				{Action: canal.InsertAction,
					Columns:   []string{"col1"},
					ColumnMap: cm,
					DocID:     3,
					Index:     "i1",
					OldRow:    []interface{}{nil},
					NewRow:    []interface{}{33731},
					PK:        newPrimaryKey([]interface{}{3}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Apply(tt.args.e)
			if (err != nil) != tt.wantErr {
				t.Errorf("IngestRule.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("IngestRule.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyRuleSet(t *testing.T) {
	type args struct {
		rs []IngestRule
		e  *canal.RowsEvent
		c  chan interface{}
	}
	testTime := time.Date(2018, 7, 11, 5, 44, 20, 483925, time.UTC)
	cm1 := map[string][]string{"f1": {"a1"}}
	cm2 := map[string][]string{"q2": {"z2"}}
	r1 := IngestRule{
		TableName:    "db1.t1",
		DocIDField:   "id",
		Index:        "i1",
		ColumnMap:    cm1,
		timeProvider: func() time.Time { return testTime },
	}
	r2 := IngestRule{
		TableName:    "db2.t2",
		DocIDField:   "id",
		Index:        "i1",
		ColumnMap:    cm2,
		timeProvider: func() time.Time { return testTime },
	}
	tests := []struct {
		name    string
		args    args
		want    []TableRowChange
		wantErr bool
	}{
		{name: "correct rule is applied when it's first",
			args: args{
				rs: []IngestRule{r1, r2},
				c:  make(chan interface{}),
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "f1"},
						},
					},
					Action: canal.DeleteAction,
					Rows:   [][]interface{}{{5, "hello"}},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.DeleteAction,
					Columns:   []string{"f1"},
					ColumnMap: cm1,
					DocID:     5,
					Index:     "i1",
					OldRow:    []interface{}{"hello"},
					NewRow:    []interface{}{nil},
					PK:        newPrimaryKey([]interface{}{5}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false,
		},
		{name: "correct rule is applied when it's not first",
			args: args{
				rs: []IngestRule{r2, r1},
				c:  make(chan interface{}),
				e: &canal.RowsEvent{
					Table: &schema.Table{
						Schema:    "db1",
						Name:      "t1",
						PKColumns: []int{0},
						Columns: []schema.TableColumn{
							{Name: "id"},
							{Name: "f1"},
						},
					},
					Action: canal.DeleteAction,
					Rows:   [][]interface{}{{5, "hello"}},
					Header: &replication.EventHeader{}}},
			want: []TableRowChange{
				{Action: canal.DeleteAction,
					Columns:   []string{"f1"},
					ColumnMap: cm1,
					DocID:     5,
					Index:     "i1",
					OldRow:    []interface{}{"hello"},
					NewRow:    []interface{}{nil},
					PK:        newPrimaryKey([]interface{}{5}),
					TableName: "db1.t1",
					TS:        testTime}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go func() {
				if _, err := ApplyRuleSet(tt.args.rs, tt.args.e, tt.args.c); (err != nil) != tt.wantErr {
					t.Errorf("ApplyRuleSet() error = %v, wantErr %v", err, tt.wantErr)
				}
				close(tt.args.c)
			}()
			got := []TableRowChange{}
			for d := range tt.args.c {
				got = append(got, d.(TableRowChange))
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("channel messages after ApplyRuleSet(): %v, want %v", got, tt.want)
			}
		})
	}
}
