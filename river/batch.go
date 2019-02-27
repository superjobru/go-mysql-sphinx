package river

import (
	"fmt"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/superjobru/go-mysql-sphinx/util"
	"gopkg.in/birkirb/loggers.v1/log"
)

// DocumentChange changed rows for single indexed document
type DocumentChange struct {
	Changes []TableRowChange
	DocID   uint64
	Index   string
	FirstTS time.Time
	LastTS  time.Time
}

// IndexChangeSet all changes related to single index
type IndexChangeSet struct {
	ChangedDocs map[uint64]DocumentChange
	Index       string
}

// BatchedChangeSet all database changes that may lead to changes in any index
type BatchedChangeSet struct {
	Changes map[string]IndexChangeSet
	Label   string
}

// BatchChangedAttrs stores the names of actually changed attrubutes
// for each document in the batch
type BatchChangedAttrs struct {
	Attrs           map[uint64]set.Set
	ChangedDocIDs   []uint64
	UnchangedDocIDs []uint64
}

// ChangeSetStats some statistics for writing to logs
type ChangeSetStats struct {
	DocCount int
	RowCount int
}

type updateQueryData struct {
	id     uint64
	fields []string
	values []string
}

type querySet struct {
	index   string
	fields  []string
	replace [][]string
	update  []updateQueryData
	delete  []uint64
}

type indexDataFromDB struct {
	fields []string
	docs   map[uint64][]string
}

// NewBatchedChangeSet create empty batched changeset
func NewBatchedChangeSet(label string) BatchedChangeSet {
	return BatchedChangeSet{
		Changes: map[string]IndexChangeSet{},
		Label:   label,
	}
}

// NewBatchedChangeSetPair create a pair for use in syncState
func NewBatchedChangeSetPair() (BatchedChangeSet, BatchedChangeSet) {
	return NewBatchedChangeSet("staged"), NewBatchedChangeSet("queue")
}

func (r *River) doBatch(batch BatchedChangeSet) error {
	if len(batch.Changes) == 0 {
		return nil
	}

	for index, indexChanges := range batch.Changes {
		log.Infof("[batch] index=%s doc_ids=%v", index, docIDList(indexChanges))
		if err := r.doIndexChangeSet(&indexChanges); err != nil {
			return err
		}
	}

	return nil
}

func (r *River) doIndexChangeSet(ics *IndexChangeSet) error {

	dataSource, exists := r.c.DataSource[ics.Index]
	if !exists {
		return errors.Errorf("no data source for '%s' index", ics.Index)
	}

	attrChangeInfo := ics.ChangedAttrsByDocID()

	if len(attrChangeInfo.ChangedDocIDs) == 0 {
		log.Infof("[batch] index=%s no changed documents", ics.Index)
		return nil
	}

	if len(attrChangeInfo.UnchangedDocIDs) > 0 {
		log.Infof("[batch] index=%s unchanged_doc_ids=%v", ics.Index, attrChangeInfo.UnchangedDocIDs)
	}

	query, err := buildSelectQuery(dataSource.details.queryTpl, attrChangeInfo.ChangedDocIDs)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[query] %s", query)

	result, err := r.canal.Execute(query)
	if err != nil {
		return errors.Trace(err)
	}

	r.StatService.ProcessedBatches.Add(1)

	indexData, err := fetchIndexData(result, dataSource)
	if err != nil {
		return errors.Trace(err)
	}

	qs := newQueryBatch(ics.Index, indexData.fields, dataSource.Parts)

	fillQueryBatch(qs, attrChangeInfo, indexData, dataSource)

	for _, qs := range qs {
		if err = r.executeQuerySet(qs); err != nil {
			return err
		}
	}

	return nil
}

func docIDList(b IndexChangeSet) []uint64 {
	ids := make([]uint64, len(b.ChangedDocs))
	i := 0
	for id := range b.ChangedDocs {
		ids[i] = id
		i++
	}
	return ids
}

func fetchIndexData(result *mysql.Result, dataSource *SourceConfig) (*indexDataFromDB, error) {
	data := &indexDataFromDB{
		fields: make([]string, len(result.Fields)),
		docs:   map[uint64][]string{},
	}

	for i, field := range result.Fields {
		data.fields[i] = string(field.Name)
	}

	for rowNo := 0; rowNo < result.RowNumber(); rowNo++ {
		id, docRow, err := fetchRow(result, dataSource, rowNo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		data.docs[id] = docRow
	}

	return data, nil
}

func fetchRow(result *mysql.Result, dataSource *SourceConfig, rowNo int) (uint64, []string, error) {
	var id uint64
	docRow := make([]string, len(result.Fields))
	for fieldName, colNo := range result.FieldNames {
		var err error
		var dbFieldExpr string

		dbFieldType, dbFieldExists := dataSource.details.fieldTypes[fieldName]
		if !dbFieldExists {
			return id, docRow, errors.Errorf("field '%s' is not specified in config", fieldName)
		}

		if dbFieldType == DocID {
			id, err = result.GetUint(rowNo, colNo)
			dbFieldExpr = fmt.Sprintf("%d", id)
		} else {
			dbFieldExpr, err = getDBValueExpression(result, dbFieldType, rowNo, colNo)
		}

		if err != nil {
			return id, docRow, errors.Trace(err)
		}

		docRow[colNo] = dbFieldExpr

		if err != nil {
			return id, docRow, errors.Trace(err)
		}
	}
	return id, docRow, nil
}

func (current DocumentChange) mergeWith(latest DocumentChange) DocumentChange {
	current.Changes = append(current.Changes, latest.Changes...)
	if current.LastTS.Before(latest.LastTS) {
		current.LastTS = latest.LastTS
	}
	return current
}

func (ics *IndexChangeSet) addDocumentChange(dc DocumentChange) {
	changeBatch, exists := ics.ChangedDocs[dc.DocID]
	if exists {
		ics.ChangedDocs[dc.DocID] = changeBatch.mergeWith(dc)
	} else {
		ics.ChangedDocs[dc.DocID] = dc
	}
}

// ChangedAttrsByDocID returns a structure that's later used for assembling a data query
func (ics *IndexChangeSet) ChangedAttrsByDocID() BatchChangedAttrs {
	attrs := map[uint64]set.Set{}
	changed := make([]uint64, 0, len(ics.ChangedDocs))
	unchanged := make([]uint64, 0, len(ics.ChangedDocs))
	for id, cb := range ics.ChangedDocs {
		attrs[id] = ChangedAttrs(cb.Changes)
		if attrs[id].Cardinality() > 0 {
			changed = append(changed, id)
		} else {
			unchanged = append(unchanged, id)
		}
	}
	return BatchChangedAttrs{
		Attrs:           attrs,
		ChangedDocIDs:   changed,
		UnchangedDocIDs: unchanged,
	}
}

// AddDocumentChange add a document change to change queue
func (b *BatchedChangeSet) AddDocumentChange(dc DocumentChange) {
	index := dc.Index
	ics, exists := b.Changes[index]
	if exists {
		ics.addDocumentChange(dc)
	} else {
		b.Changes[index] = IndexChangeSet{
			Index: index,
			ChangedDocs: map[uint64]DocumentChange{
				dc.DocID: dc,
			},
		}
	}
}

// AddRowChange add a row change to change queue
func (b *BatchedChangeSet) AddRowChange(d TableRowChange) {
	b.AddDocumentChange(DocumentChange{
		Changes: []TableRowChange{d},
		DocID:   d.DocID,
		Index:   d.Index,
		FirstTS: d.TS,
		LastTS:  d.TS,
	})
}

// Stats how many documents have pending changes for each index
func (b BatchedChangeSet) Stats() map[string]ChangeSetStats {
	m := map[string]ChangeSetStats{}
	for index, changeSet := range b.Changes {
		rowCount := 0
		for _, doc := range changeSet.ChangedDocs {
			rowCount += len(doc.Changes)
		}
		m[index] = ChangeSetStats{
			DocCount: len(changeSet.ChangedDocs),
			RowCount: rowCount,
		}
	}
	return m
}

// FirstSeenPositionEventID finds lowest EventID field value among contained
// TableRowChange instances; if this batched set contains no changes, returns lastID.
// This method is used to determine a position which could be safely assumed to be fully applied to Sphinx.
func (b BatchedChangeSet) FirstSeenPositionEventID(lastID uint64) uint64 {
	var id uint64 = 0xffffffffffffffff
	found := false
	for _, changeSet := range b.Changes {
		for _, doc := range changeSet.ChangedDocs {
			for _, rowChange := range doc.Changes {
				if id > rowChange.EventID {
					id = rowChange.EventID
					found = true
				}
			}
		}
	}
	if found {
		return id
	}
	return lastID
}

func newQueryBatch(index string, fields []string, parts uint16) []*querySet {
	qs := make([]*querySet, parts)
	var chunkNo uint16
	for chunkNo = 0; chunkNo < parts; chunkNo++ {
		qs[chunkNo] = &querySet{
			index:   util.IndexChunkName(index, parts, chunkNo),
			fields:  fields,
			replace: [][]string{},
			update:  []updateQueryData{},
			delete:  []uint64{},
		}
	}
	return qs
}

func fillQueryBatch(
	qs []*querySet,
	attrInfo BatchChangedAttrs,
	indexData *indexDataFromDB,
	dataSource *SourceConfig,
) {

	for _, id := range attrInfo.ChangedDocIDs {
		docData, exists := indexData.docs[id]
		q := qs[util.IndexChunk(id, dataSource.Parts)]
		if !exists {
			q.delete = append(q.delete, id)
		} else {
			docAttrs := attrInfo.Attrs[id]
			if docAttrs.IsSubset(dataSource.details.attrFields) {
				attrCount := docAttrs.Cardinality()
				fields := make([]string, 0, attrCount)
				values := make([]string, 0, attrCount)
				for colNo, f := range indexData.fields {
					if docAttrs.Contains(f) {
						fields = append(fields, f)
						values = append(values, docData[colNo])
					}
				}
				q.update = append(q.update, updateQueryData{
					id:     id,
					fields: fields,
					values: values,
				})
			} else {
				q.replace = append(q.replace, docData)
			}
		}
	}
}

func (r *River) executeQuerySet(q *querySet) error {
	st := r.StatService
	if len(q.replace) > 0 {
		query := formatReplaceQuery(q.index, q.fields, q.replace)
		err := r.sphinxService.Query(query, &st.ReplaceAffectedRows)
		if err != nil {
			return errors.Trace(err)
		}
		st.ReplaceQueries.Add(1)
		st.ReplaceQueryDocs.Add(uint64(len(q.replace)))
	}

	if len(q.delete) > 0 {
		query := formatDeleteQuery(q.index, q.delete)
		err := r.sphinxService.Query(query, &st.DeleteAffectedRows)
		if err != nil {
			return errors.Trace(err)
		}
		st.DeleteQueries.Add(1)
		st.DeleteQueryDocs.Add(uint64(len(q.delete)))
	}

	for _, update := range q.update {
		query := formatUpdateQuery(q.index, update.id, update.fields, update.values)
		err := r.sphinxService.Query(query, &st.UpdateAffectedRows)
		if err != nil {
			return errors.Trace(err)
		}
		st.UpdateQueries.Add(1)
	}

	return nil
}
