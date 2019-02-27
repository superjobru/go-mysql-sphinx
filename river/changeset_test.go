package river

import (
	"testing"

	"github.com/siddontang/go-mysql/canal"

	set "github.com/deckarep/golang-set"
)

func TestChangedColumns(t *testing.T) {
	type args struct {
		changeList []TableRowChange
	}
	cm1 := map[string][]string{
		"a1": {"attr1"},
		"a2": {"attr1", "attr2"},
		"t1": {"field1"},
		"t2": {"field2"},
	}
	cm2 := map[string][]string{"a1": {"attr1"}}
	tests := []struct {
		name string
		args args
		want set.Set
	}{
		{name: "no row changes",
			args: args{changeList: []TableRowChange{}},
			want: set.NewSet()},
		{name: "one simple change",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"id", "a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{nil, nil, nil, nil, nil},
					NewRow:    []interface{}{1, 123, 234, "hello", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1", "attr2", "field1", "field2")},
		{name: "one changed value",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"id", "a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{1, 111, 234, "hello", "world"},
					NewRow:    []interface{}{1, 123, 234, "hello", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1")},
		{name: "one simple change with byte slice value",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"id", "a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{nil, nil, nil, nil, nil},
					NewRow:    []interface{}{1, 123, 234, []uint8("hello"), "world"},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1", "attr2", "field1", "field2")},
		{name: "one change with null value",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"id", "a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{nil, nil, nil, nil, nil},
					NewRow:    []interface{}{1, 123, nil, "hello", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1", "field1", "field2")},
		{name: "several changes in the same one-to-one table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{123, 234, "hello", "world"},
					NewRow:    []interface{}{717, 456, "hello", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{717, 456, "hello", "world"},
					NewRow:    []interface{}{717, 567, "goodbye", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"a1", "a2", "t1", "t2"},
					ColumnMap: cm1,
					OldRow:    []interface{}{717, 567, "goodbye", "world"},
					NewRow:    []interface{}{717, 234, "goodbye", "world"},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1", "field1")},
		{name: "one insert to one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1, 717})},
			}},
			want: set.NewSet("attr1")},
		{name: "one insert with byte slice value to one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "f1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, []uint8("hello")},
					PK:        newPrimaryKey([]interface{}{1, "hello"})},
			}},
			want: set.NewSet()},
		{name: "insert then update in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 717, 1234},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, 1234},
					NewRow:    []interface{}{1, 717, 2345},
					PK:        newPrimaryKey([]interface{}{1, 717})},
			}},
			want: set.NewSet("attr1")},
		{name: "insert then delete in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
			}},
			want: set.NewSet()},
		{name: "multiple insert then delete in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 717, 234},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 719, 233},
					PK:        newPrimaryKey([]interface{}{1, 719})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 720, 233},
					PK:        newPrimaryKey([]interface{}{1, 720})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, 234},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720, 233},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 720})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719, 233},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 719})},
			}},
			want: set.NewSet()},
		{name: "delete then insert in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1, 717})},
			}},
			want: set.NewSet()},
		{name: "delete then insert in one-to-one table with proper primary key with no change",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet()},
		{name: "delete then insert in one-to-one table with proper primary key leading to a change",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 718},
					PK:        newPrimaryKey([]interface{}{1})},
			}},
			want: set.NewSet("attr1")},
		{name: "multiple delete then insert in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 720})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 719})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 719},
					PK:        newPrimaryKey([]interface{}{1, 719})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 720},
					PK:        newPrimaryKey([]interface{}{1, 720})},
			}},
			want: set.NewSet()},
		{name: "multiple delete then insert leading to a change in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 720})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 719})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 719},
					PK:        newPrimaryKey([]interface{}{1, 719})},
			}},
			want: set.NewSet("attr1")},
		{name: "insert then update then delete in one-to-many table",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "a2"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 717, "yay"},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, "yay"},
					NewRow:    []interface{}{1, 717, "hey"},
					PK:        newPrimaryKey([]interface{}{1, 717})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, "hey"},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{1, 717})},
			}},
			want: set.NewSet()},
		{name: "one insert to one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{29387429})},
			}},
			want: set.NewSet("attr1")},
		{name: "one insert with byte slice value to one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "f1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, []uint8("hello")},
					PK:        newPrimaryKey([]interface{}{29387429})},
			}},
			want: set.NewSet()},
		{name: "insert then update in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 717, 1234},
					PK:        newPrimaryKey([]interface{}{29387429})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, 1234},
					NewRow:    []interface{}{1, 717, 2345},
					PK:        newPrimaryKey([]interface{}{29387429})},
			}},
			want: set.NewSet("attr1")},
		{name: "insert then delete in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{29387429})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{29387429})},
			}},
			want: set.NewSet()},
		{name: "multiple insert then delete in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 717, 234},
					PK:        newPrimaryKey([]interface{}{938457})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 719, 233},
					PK:        newPrimaryKey([]interface{}{938458})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil, nil},
					NewRow:    []interface{}{1, 720, 233},
					PK:        newPrimaryKey([]interface{}{938459})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717, 234},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{938457})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720, 233},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{938459})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1", "q1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719, 233},
					NewRow:    []interface{}{nil, nil, nil},
					PK:        newPrimaryKey([]interface{}{938458})},
			}},
			want: set.NewSet()},
		{name: "delete then insert in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938458})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{938459})},
			}},
			want: set.NewSet()},
		{name: "multiple delete then insert in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938451})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938452})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938453})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{938454})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 719},
					PK:        newPrimaryKey([]interface{}{938455})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 720},
					PK:        newPrimaryKey([]interface{}{938456})},
			}},
			want: set.NewSet()},
		{name: "multiple delete then insert leading to a change in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938451})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 720},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938453})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 719},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{938452})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{938454})},
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 719},
					PK:        newPrimaryKey([]interface{}{938455})},
			}},
			want: set.NewSet("attr1")},
		{name: "insert then update then delete in one-to-many table with surrogate key",
			args: args{changeList: []TableRowChange{
				TableRowChange{
					Action:    canal.InsertAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{nil, nil},
					NewRow:    []interface{}{1, 717},
					PK:        newPrimaryKey([]interface{}{384576})},
				TableRowChange{
					Action:    canal.UpdateAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 717},
					NewRow:    []interface{}{1, 171},
					PK:        newPrimaryKey([]interface{}{384576})},
				TableRowChange{
					Action:    canal.DeleteAction,
					Columns:   []string{"doc_id", "a1"},
					ColumnMap: cm2,
					OldRow:    []interface{}{1, 171},
					NewRow:    []interface{}{nil, nil},
					PK:        newPrimaryKey([]interface{}{384576})},
			}},
			want: set.NewSet()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ChangedAttrs(tt.args.changeList); !got.Equal(tt.want) {
				t.Errorf("ChangedAttrs() = %v, want %v", got, tt.want)
			}
		})
	}
}
