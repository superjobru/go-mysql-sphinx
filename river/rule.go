package river

import (
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/superjobru/go-mysql-sphinx/util"
	"gopkg.in/birkirb/loggers.v1/log"
)

// Table relation types
const (
	IdentityRelation   = "identity"
	OneToOneRelation   = "one_to_one"
	OneToManyRelation  = "one_to_many"
	ManyToManyRelation = "many_to_many"
)

// IngestRule is the rule for how to detect changed documents in MySQL.
type IngestRule struct {
	TableName    string              `toml:"table"`
	DocIDField   string              `toml:"id_field"`
	Index        string              `toml:"index"`
	ColumnMap    map[string][]string `toml:"column_map"`
	timeProvider func() time.Time
}

// IngestTable is just a struct to avoid concatenation
type IngestTable struct {
	Schema string
	Table  string
}

func toRowValueMap(e *canal.RowsEvent) map[string]interface{} {
	var colName string
	row := map[string]interface{}{}
	for _, values := range e.Rows {
		for i, val := range values {
			colName = e.Table.Columns[i].Name
			if row[colName] == nil {
				row[colName] = val
			}
		}
	}
	return row
}

func rowToArray(row []interface{}) [128]interface{} {
	var m [128]interface{}
	for colNo, value := range row {
		m[colNo] = hashable(value)
	}
	return m
}

func columnNames(e *canal.RowsEvent) []string {
	l := make([]string, len(e.Table.Columns))
	for i, c := range e.Table.Columns {
		l[i] = c.Name
	}
	return l
}

func realPKColumns(e *canal.RowsEvent) []int {
	if len(e.Table.PKColumns) > 0 {
		return e.Table.PKColumns
	}
	cols := make([]int, len(e.Table.Columns))
	for i := range e.Table.Columns {
		cols[i] = i
	}
	return cols
}

func newPrimaryKey(values []interface{}) PrimaryKey {
	var v [16]interface{}
	for i, val := range values {
		v[i] = hashable(val)
	}
	return PrimaryKey{Len: len(values), Value: v}
}

func newPrimaryKeyFromRow(pkCols []int, row []interface{}) PrimaryKey {
	var v [16]interface{}
	for i, colNo := range pkCols {
		v[i] = hashable(row[colNo])
	}
	return PrimaryKey{Len: len(pkCols), Value: v}
}

func (r *IngestRule) check(sources map[string]*SourceConfig) error {
	dataSource, exists := sources[r.Index]
	if !exists {
		return errors.Errorf("no datasource for index '%s'", r.Index)
	}
	if !dataSource.hasTable(r.TableName) {
		return errors.Errorf("table '%s' is absent in datasource for index '%s'", r.TableName, r.Index)
	}
	if len(r.ColumnMap) == 0 {
		return errors.Errorf("empty column_map")
	}
	return r.checkColumnMap(dataSource)
}

func (r *IngestRule) checkColumnMap(dataSource *SourceConfig) error {
	for _, colMap := range r.ColumnMap {
		for _, field := range colMap {
			_, fieldExists := dataSource.details.fieldTypes[field]
			if !fieldExists {
				return errors.Errorf("unknown field '%s'", field)
			}
		}
	}
	return nil
}

// Apply converts row event to document changeset
// according to a single rule
func (r *IngestRule) Apply(e *canal.RowsEvent) ([]TableRowChange, error) {
	if r.TableName != e.Table.String() {
		return nil, nil
	}
	idColNo, err := fieldIndex(e, r.DocIDField)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch e.Action {
	case canal.InsertAction:
		return r.makeInsertChangeSet(e, idColNo)
	case canal.UpdateAction:
		return r.makeUpdateChangeSet(e, idColNo)
	case canal.DeleteAction:
		return r.makeDeleteChangeSet(e, idColNo)
	}
	panic("unknown row event action")
}

// ApplyRuleSet converts row event to document changeset and sends to the channel c
func ApplyRuleSet(ruleSet []IngestRule, e *canal.RowsEvent, c chan interface{}) (uint64, error) {
	var docCount uint64
	for ruleID := range ruleSet {
		rule := ruleSet[ruleID]
		docs, err := rule.Apply(e)
		if err != nil {
			return docCount, errors.Trace(err)
		}
		if docs != nil {
			log.Infof(
				"[row event] ruleId=%d index=%s table=%s action=%s rows=%d docs=%v",
				ruleID,
				rule.Index,
				e.Table,
				e.Action,
				rowCount(e),
				DocIDList(docs),
			)
			for _, doc := range docs {
				c <- doc
				docCount++
			}
		}
	}
	return docCount, nil
}

func (r *IngestRule) makeInsertChangeSet(e *canal.RowsEvent, idColNo int) ([]TableRowChange, error) {
	c := make([]TableRowChange, len(e.Rows))
	i := 0
	columns := columnNames(e)
	pkColumns := realPKColumns(e)
	for _, row := range e.Rows {
		docID, err := util.CoerceToUint64(row[idColNo])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if docID == 0 {
			log.Errorf("[insert] could not get doc id: %s", spew.Sdump(e))
		}
		c[i] = r.insertedTableRow(docID, pkColumns, columns, row)
		i++
	}
	return c, nil
}

func (r *IngestRule) makeUpdateChangeSet(e *canal.RowsEvent, idColNo int) ([]TableRowChange, error) {
	c := make([]TableRowChange, 0, len(e.Rows))
	columns := columnNames(e)
	pkColumns := realPKColumns(e)
	for i := 0; i < len(e.Rows)/2; i++ {
		oldRow := e.Rows[i*2]
		newRow := e.Rows[i*2+1]
		oldDocID, err := util.CoerceToUint64(oldRow[idColNo])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if oldDocID == 0 {
			log.Errorf("[update/old] could not get doc id: %s", spew.Sdump(e))
		}
		var newDocID uint64
		if newRow[idColNo] == nil {
			newDocID = oldDocID
		} else {
			newDocID, err = util.CoerceToUint64(newRow[idColNo])
			if err != nil {
				return nil, errors.Trace(err)
			}
			if newDocID == 0 {
				log.Errorf("[update/new] could not get doc id: %s", spew.Sdump(e))
			}
		}
		if oldDocID == newDocID {
			c = append(c, r.updatedTableRow(newDocID, pkColumns, columns, oldRow, newRow))
		} else {
			c = append(c, r.deletedTableRow(oldDocID, pkColumns, columns, oldRow))
			c = append(c, r.insertedTableRow(newDocID, pkColumns, columns, newRow))
		}
	}
	return c, nil
}

func (r *IngestRule) makeDeleteChangeSet(e *canal.RowsEvent, idColNo int) ([]TableRowChange, error) {
	c := make([]TableRowChange, len(e.Rows))
	i := 0
	columns := columnNames(e)
	pkColumns := realPKColumns(e)
	for _, row := range e.Rows {
		docID, err := util.CoerceToUint64(row[idColNo])
		if err != nil {
			return nil, errors.Trace(err)
		}
		if docID == 0 {
			log.Errorf("[delete] could not get doc id: %s", spew.Sdump(e))
		}
		c[i] = r.deletedTableRow(docID, pkColumns, columns, row)
		i++
	}
	return c, nil
}

func emptyRow(row []interface{}) []interface{} {
	return make([]interface{}, len(row))
}

func (r *IngestRule) filterColumns(columns []string, row []interface{}) ([]string, []interface{}) {
	filteredColumns := make([]string, len(r.ColumnMap))
	filteredRow := make([]interface{}, len(r.ColumnMap))
	i := 0
	for colNo, col := range columns {
		_, isIndexed := r.ColumnMap[col]
		if isIndexed {
			filteredColumns[i] = col
			filteredRow[i] = row[colNo]
			i++
		}
	}
	return filteredColumns, filteredRow
}

func (r *IngestRule) insertedTableRow(docID uint64, pkCols []int, columns []string, row []interface{}) TableRowChange {
	pk := newPrimaryKeyFromRow(pkCols, row)
	columns, row = r.filterColumns(columns, row)
	return TableRowChange{
		Action:    canal.InsertAction,
		Columns:   columns,
		ColumnMap: r.ColumnMap,
		DocID:     docID,
		Index:     r.Index,
		OldRow:    emptyRow(row),
		NewRow:    row,
		PK:        pk,
		TableName: r.TableName,
		TS:        r.timeProvider(),
	}
}

func (r *IngestRule) updatedTableRow(docID uint64, pkCols []int, columns []string, oldRow []interface{}, newRow []interface{}) TableRowChange {
	pk := newPrimaryKeyFromRow(pkCols, oldRow)
	_, oldRow = r.filterColumns(columns, oldRow)
	columns, newRow = r.filterColumns(columns, newRow)
	return TableRowChange{
		Action:    canal.UpdateAction,
		Columns:   columns,
		ColumnMap: r.ColumnMap,
		DocID:     docID,
		Index:     r.Index,
		OldRow:    oldRow,
		NewRow:    newRow,
		PK:        pk,
		TableName: r.TableName,
		TS:        r.timeProvider(),
	}
}

func (r *IngestRule) deletedTableRow(docID uint64, pkCols []int, columns []string, row []interface{}) TableRowChange {
	pk := newPrimaryKeyFromRow(pkCols, row)
	columns, row = r.filterColumns(columns, row)
	return TableRowChange{
		Action:    canal.DeleteAction,
		Columns:   columns,
		ColumnMap: r.ColumnMap,
		DocID:     docID,
		Index:     r.Index,
		OldRow:    row,
		NewRow:    emptyRow(row),
		PK:        pk,
		TableName: r.TableName,
		TS:        r.timeProvider(),
	}
}

func fieldIndex(e *canal.RowsEvent, fieldName string) (int, error) {
	for i, col := range e.Table.Columns {
		if fieldName == col.Name {
			return i, nil
		}
	}
	return -1, errors.Errorf("field %s not found in binlog event %s", fieldName, spew.Sdump(e))
}
