package river

import (
	"reflect"

	"github.com/siddontang/go-mysql/canal"

	set "github.com/deckarep/golang-set"
)

// ColumnValueDiff changes for single document in a single database table+column
type ColumnValueDiff interface {
	Init(v ValuePair)
	ApplyValue(v ValuePair)
	IsEmpty() bool
}

// PlainValueDiff tuple representing a changed database value
// `OldValue` and `NewValue` should be different
type PlainValueDiff struct {
	OldValue interface{}
	NewValue interface{}
}

// SetValueDiff tuple representing a changed database value set
// `Added` and `Deleted` sets should be different
// For now does not depend on `TableRowChange.Action` and relies only on value changes.
// As a consequence, inserted/deleted nil values are completely ignored.
type SetValueDiff struct {
	Added   set.Set
	Deleted set.Set
}

// RowDiff tuple representing a changed database row
// `Old` and `New` may be the same
type RowDiff struct {
	IsInsert bool
	IsUpdate bool
	IsDelete bool
	Old      []interface{}
	New      []interface{}
}

// TableChangeSet changes for single document for a table in a single database table
// `Diff` map should contain only actual (non-empty) changes
type TableChangeSet struct {
	Columns   []string
	ColumnMap map[string][]string
	Changes   map[PrimaryKey]RowDiff
}

// DocChangeSet represents changes of a single document in all related tables
type DocChangeSet struct {
	Changes map[string]TableChangeSet
}

// ChangedAttrs returns a set of names of attributes of a document
// whose values were changed after applying changeList
func ChangedAttrs(changeList []TableRowChange) set.Set {
	attrs := set.NewSet()
	dcs := NewDocChangeSet(changeList)
	for _, tcs := range dcs.Changes {
		for _, col := range tcs.ChangedColumnNames() {
			for _, attr := range tcs.ColumnMap[col] {
				attrs.Add(attr)
			}
		}
	}
	return attrs
}

// NewDocChangeSet creates a DocChangeSet
// We probably could avoid that, but to determine which index fields/attributes
// should be changed, we should first find out which columns actually changed.
func NewDocChangeSet(changeList []TableRowChange) DocChangeSet {
	s := DocChangeSet{Changes: map[string]TableChangeSet{}}
	for _, c := range changeList {
		s.ApplyChange(c)
	}
	s.PruneUnchangedTables()
	return s
}

// ApplyChange modifies DocChangeSet according to changes in a table row
func (s *DocChangeSet) ApplyChange(c TableRowChange) {
	table := c.TableName
	tableChangeSet, exists := s.Changes[table]
	if !exists {
		tableChangeSet = TableChangeSet{
			Columns:   c.Columns,
			ColumnMap: c.ColumnMap,
			Changes:   map[PrimaryKey]RowDiff{},
		}
		s.Changes[table] = tableChangeSet
	}
	tableChangeSet.ApplyChange(c)
}

// PruneUnchangedTables removes TableChangeSets that do not contain changed values
func (s *DocChangeSet) PruneUnchangedTables() {
	for table, tcs := range s.Changes {
		tcs.PruneUnchangedRows()
		if len(tcs.Changes) == 0 {
			delete(s.Changes, table)
		}
	}
}

// ApplyChange modifies TableChangeSet according to changes in a table row
func (t *TableChangeSet) ApplyChange(c TableRowChange) {
	diff, exists := t.Changes[c.PK]
	if exists {
		isInsert := diff.IsInsert && c.Action != canal.DeleteAction
		isDelete := !diff.IsInsert && c.Action == canal.DeleteAction
		t.Changes[c.PK] = RowDiff{
			IsInsert: isInsert,
			IsUpdate: !isInsert && !isDelete,
			IsDelete: isDelete,
			Old:      diff.Old,
			New:      c.NewRow,
		}
	} else {
		t.Changes[c.PK] = RowDiff{
			IsInsert: c.Action == canal.InsertAction,
			IsUpdate: c.Action == canal.UpdateAction,
			IsDelete: c.Action == canal.DeleteAction,
			Old:      c.OldRow,
			New:      c.NewRow,
		}
	}
}

// PruneUnchangedRows removes RowDiffs that do not contain changed values
func (t *TableChangeSet) PruneUnchangedRows() {
	for pk, diff := range t.Changes {
		if reflect.DeepEqual(diff.Old, diff.New) {
			delete(t.Changes, pk)
		}
	}
}

// ChangedColumnNames returns names of the columns where values actually has changed
func (t *TableChangeSet) ChangedColumnNames() []string {
	before := set.NewSet()
	after := set.NewSet()
	for _, diff := range t.Changes {
		if diff.IsDelete || diff.IsUpdate {
			before.Add(rowToArray(diff.Old))
		}
		if diff.IsInsert || diff.IsUpdate {
			after.Add(rowToArray(diff.New))
		}
	}

	if after.Equal(before) {
		return make([]string, 0)
	}

	if after.Cardinality() > 1 || before.Cardinality() > 1 {
		return t.Columns
	}

	columnSet := map[string]struct{}{}
	for _, diff := range t.Changes {
		for colNo, col := range t.Columns {
			if !reflect.DeepEqual(diff.Old[colNo], diff.New[colNo]) {
				columnSet[col] = struct{}{}
			}
		}
	}
	columns := make([]string, len(columnSet))
	i := 0
	for col := range columnSet {
		columns[i] = col
		i++
	}
	return columns
}

// NewColumnValueDiff create a suitable ColumnValueDiff according to relation type
func NewColumnValueDiff(v ValuePair, relation string) ColumnValueDiff {
	var diff ColumnValueDiff
	switch relation {
	case IdentityRelation, OneToOneRelation:
		diff = &PlainValueDiff{}
	case OneToManyRelation:
		diff = &SetValueDiff{}
	default:
		panic("unknown table relation type")
	}
	diff.Init(v)
	return diff
}

// Init ColumnValueDiff implementation
func (d *PlainValueDiff) Init(v ValuePair) {
	d.OldValue = v.Old
	d.NewValue = v.New
}

// ApplyValue ColumnValueDiff implementation
func (d *PlainValueDiff) ApplyValue(v ValuePair) {
	d.NewValue = v.New
}

// IsEmpty ColumnValueDiff implementation
func (d *PlainValueDiff) IsEmpty() bool {
	v := ValuePair{
		Old: d.OldValue,
		New: d.NewValue,
	}
	return v.HasSameValues()
}

// Init ColumnValueDiff implementation
func (d *SetValueDiff) Init(v ValuePair) {
	d.Added = set.NewSet()
	d.Deleted = set.NewSet()
	if v.Old != nil {
		d.Deleted.Add(v.Old)
	}
	if v.New != nil {
		d.Added.Add(v.New)
	}
}

// ApplyValue ColumnValueDiff implementation
func (d *SetValueDiff) ApplyValue(v ValuePair) {
	if v.HasSameValues() {
		return
	}
	if v.Old != nil {
		if d.Added.Contains(v.Old) {
			d.Added.Remove(v.Old)
		} else {
			d.Deleted.Add(v.Old)
		}
	}
	if v.New != nil {
		if d.Deleted.Contains(v.New) {
			d.Deleted.Remove(v.New)
		} else {
			d.Added.Add(v.New)
		}
	}
}

// IsEmpty ColumnValueDiff implementation
func (d *SetValueDiff) IsEmpty() bool {
	return d.Added.Equal(d.Deleted)
}
