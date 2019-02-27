package river

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	uuid "github.com/satori/go.uuid"
)

func testConfigForChunk(config string, chunk int) []byte {
	config = strings.Replace(config,
		"test_index_ftw_plain",
		fmt.Sprintf("test_index_ftw_plain_part_%d", chunk), -1)
	config = strings.Replace(config,
		"from test as t",
		fmt.Sprintf("from test as t where t.id %% 3 = %d", chunk), -1)

	return []byte(config)
}

func testBuildForChunk(chunk indexChunkBuild, chunkNo int) indexChunkBuild {
	return indexChunkBuild{
		buildID:    testUUIDString,
		config:     testConfigForChunk(string(chunk.config), chunkNo),
		files:      chunk.files,
		plainIndex: fmt.Sprintf("test_index_ftw_plain_part_%d", chunkNo),
		rtIndex:    fmt.Sprintf("test_index_ftw_part_%d", chunkNo),
	}
}

const testUUIDString = "00112233-4455-6677-8899-aabbccddeeff"

func testUUID() uuid.UUID {
	return uuid.Must(uuid.FromString(testUUIDString))
}

func Test_newIndexBuild(t *testing.T) {
	query := strings.Join([]string{
		"SELECT t.id AS `:id`,",
		"t.f1 AS `:attr_uint`,",
		"t.f2 AS `:attr_bigint`,",
		"t.f3 AS `:attr_float`,",
		"t.f4 AS `f4:attr_multi`,",
		"CONCAT(t.f5, t.f10) AS `f5:attr_string`,",
		"t.f6 AS `:field`,",
		"t.f7 AS `m5:attr_multi_64`",
		"FROM test t",
	}, " ")
	config := `
indexer {
	mem_limit = 128M
	write_buffer = 4M
}

source test_index_ftw_plain
{
	type = mysql
	sql_host = db1
	sql_port = 3306
	sql_user = dev
	sql_pass = yo

	sql_query_pre = SET NAMES utf8
	sql_query_pre = SET group_concat_max_len = 12345

	sql_db =
	sql_query = select t.id as id, t.f1 as f1, t.f2 as f2, t.f3 as f3, t.f4 as f4, CONCAT(t.f5, t.f10) as f5, t.f6 as f6, t.f7 as m5 from test as t

	# sql_id = id
	sql_attr_uint = f1
	sql_attr_bigint = f2
	sql_attr_float = f3
	sql_attr_multi = uint f4 from field
	sql_attr_string = f5
	# sql_field = f6
	sql_attr_multi = bigint m5 from field
}

index test_index_ftw_plain
{
	type = plain
	exceptions = test_index_ftw_plain.dictionary.dict1.txt
	stopwords = test_index_ftw_plain.dictionary.dict2.txt
	wordforms = test_index_ftw_plain.dictionary.dict3.txt
	morphology = stem_en
	path = test_index_ftw_plain.new
	source = test_index_ftw_plain
}
`
	src1 := SourceConfig{
		Query: query,
		Parts: 1,
		Indexer: IndexerConfig{
			MemLimit:          "128M",
			WriteBuffer:       "4M",
			GroupConcatMaxLen: 12345,
			Tokenizer:         "morphology = stem_en",
			Dictionaries: IndexDictionaries{
				Exceptions: "/path/to/dict1.txt",
				Stopwords:  "/path/to/dict2.txt",
				Wordforms:  "/path/to/dict3.txt",
			},
		},
	}
	src2 := src1
	src2.Parts = 3
	chunk := indexChunkBuild{
		buildID:    testUUIDString,
		config:     []byte(config),
		plainIndex: "test_index_ftw_plain",
		rtIndex:    "test_index_ftw",
		files: []string{
			"/path/to/dict1.txt",
			"/path/to/dict2.txt",
			"/path/to/dict3.txt",
		},
	}
	type args struct {
		c     *Config
		index string
	}
	tests := []struct {
		name    string
		args    args
		want    *indexBuild
		wantErr bool
	}{
		{
			name: "one chunk",
			args: args{
				c: &Config{
					MyAddr:     "db1",
					MyUser:     "dev",
					MyPassword: "yo",
					MyCharset:  "utf8",
					DataSource: map[string]*SourceConfig{
						"test_index_ftw": &src1,
					},
				},
				index: "test_index_ftw",
			},
			want: &indexBuild{
				id:     testUUIDString,
				index:  "test_index_ftw",
				chunks: []indexChunkBuild{chunk},
				parts:  1,
			},
		},
		{
			name: "many chunks",
			args: args{
				c: &Config{
					MyAddr:     "db1",
					MyUser:     "dev",
					MyPassword: "yo",
					MyCharset:  "utf8",
					DataSource: map[string]*SourceConfig{
						"test_index_ftw": &src2,
					},
				},
				index: "test_index_ftw",
			},
			want: &indexBuild{
				id:    testUUIDString,
				index: "test_index_ftw",
				chunks: []indexChunkBuild{
					testBuildForChunk(chunk, 0),
					testBuildForChunk(chunk, 1),
					testBuildForChunk(chunk, 2),
				},
				parts: 3,
			},
		},
		{
			name: "wrong index",
			args: args{
				c:     &Config{},
				index: "does_not_exist",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newIndexBuild(tt.args.c, tt.args.index, testUUID)
			if (err != nil) != tt.wantErr {
				t.Errorf("indexerConfigSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("indexerConfigSet() = %s, want %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}
