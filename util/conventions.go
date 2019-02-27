package util

import (
	"fmt"
	"path/filepath"
)

// IndexChunkName name of a "chunk" (or "partition") of an RT-index as it's registered in Sphinx
func IndexChunkName(index string, parts, chunkNo uint16) string {
	if parts > 1 {
		// TODO: make the pattern configurable
		return fmt.Sprintf("%s_part_%d", index, chunkNo)
	}
	return index
}

// IndexChunk returns an id of the index chunk, which should contain a document with the given id
func IndexChunk(id uint64, parts uint16) uint16 {
	// TODO: make the bucketing function configurable
	return uint16(id % uint64(parts))
}

// PlainIndexName name of "plain" counterpart of an RT-index
// This is neccessary for seamless rebuilding.
func PlainIndexName(index string) string {
	return fmt.Sprintf("%s_plain", index)
}

// DictionaryTmpFilename actual file name for a dictionary file as it's used by indexer
func DictionaryTmpFilename(index, file string) string {
	return fmt.Sprintf("%s.dictionary.%s", index, filepath.Base(file))
}
