package river

import (
	"fmt"
	"strings"
)

func formatReplaceQuery(index string, fields []string, values [][]string) string {
	set := make([]string, len(values))
	for i, data := range values {
		set[i] = fmt.Sprintf("(%s)", strings.Join(data, ","))
	}
	return fmt.Sprintf(
		"REPLACE INTO %s (%s) VALUES %s",
		index,
		strings.Join(fields, ","),
		strings.Join(set, ","))
}

func formatUpdateQuery(index string, id uint64, fields []string, values []string) string {
	setClause := make([]string, 0, len(fields))
	for i, f := range fields {
		setClause = append(setClause, fmt.Sprintf("%s = %s", f, values[i]))
	}
	return fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = %d",
		index,
		strings.Join(setClause, ","),
		id)
}

func formatDeleteQuery(index string, docIDs []uint64) string {
	ids := make([]string, len(docIDs))
	for i, id := range docIDs {
		ids[i] = fmt.Sprintf("%d", id)
	}
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id IN (%s)",
		index,
		strings.Join(ids, ","))
}
