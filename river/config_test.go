package river

import (
	"reflect"
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type configTestSuite struct {
	c *Config
}

var _ = Suite(&configTestSuite{})

func (s *configTestSuite) TestSimpleSelect(c *C) {
	d, err := parseQuery("SELECT t.id AS `:id` FROM test t")
	c.Assert(err, IsNil)
	c.Assert(d.fieldTypes, DeepEquals, map[string]string{"id": DocID})
	q, err := buildSelectQuery(d.queryTpl, []uint64{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(q, Equals, "select t.id as id from test as t where t.id in (1, 2, 3)")
}

func (s *configTestSuite) TestAnotherSelect(c *C) {
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
	d, err := parseQuery(query)
	c.Assert(err, IsNil)
	c.Assert(d.fieldTypes, DeepEquals, map[string]string{
		"id": DocID,
		"f1": AttrUint,
		"f2": AttrBigint,
		"f3": AttrFloat,
		"f4": AttrMulti,
		"f5": AttrString,
		"f6": TextField,
		"m5": AttrMulti64,
	})
	q, err := buildSelectQuery(d.queryTpl, []uint64{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(q, Equals, strings.Join([]string{
		"select t.id as id,",
		"t.f1 as f1,",
		"t.f2 as f2,",
		"t.f3 as f3,",
		"t.f4 as f4,",
		"CONCAT(t.f5, t.f10) as f5,",
		"t.f6 as f6,",
		"t.f7 as m5",
		"from test as t where t.id in (1, 2, 3)",
	}, " "))
}

func (s *configTestSuite) TestInvalidSelect(c *C) {
	_, err := parseQuery("SELECT to be or not to be FROM test")
	c.Assert(err, ErrorMatches, "failed to parse SQL query.*")
}

func (s *configTestSuite) TestSelectWithoutFrom(c *C) {
	_, err := parseQuery("SELECT 1 AS `:id`")
	c.Assert(err.Error(), Equals, "SQL query must have a FROM clause")
}

func (s *configTestSuite) TestUpdateInsteadOfSelect(c *C) {
	_, err := parseQuery("UPDATE test SET a = 1")
	c.Assert(err.Error(), Equals, "expected a SELECT query, but got *sqlparser.Update")
}

func (s *configTestSuite) TestAbsentAlias(c *C) {
	_, err := parseQuery("SELECT id FROM test")
	c.Assert(err.Error(), Equals, "select expression 'id' must have an alias")
}

func (s *configTestSuite) TestInvalidColumnAlias(c *C) {
	_, err := parseQuery("SELECT id AS `id` FROM test")
	c.Assert(err.Error(), Equals, "alias 'id' in expression 'id as id' must conform to '[ColumnName]:ColumnType' format")
}

func (s *configTestSuite) TestInvalidExpressionAlias(c *C) {
	_, err := parseQuery("SELECT id + 1 AS `:id` FROM test")
	c.Assert(err.Error(), Equals, "alias ':id' in expression 'id + 1 as `:id`' must conform to 'ColumnName:ColumnType' format (since the aliased expression is not a column name)")
}

func (s *configTestSuite) TestInvalidColumnType(c *C) {
	_, err := parseQuery("SELECT id AS `:id`, f AS `:attr_wtf` FROM test")
	c.Assert(err.Error(), Equals, "invalid column alias ':attr_wtf': invalid index column type: attr_wtf")
}

func (s *configTestSuite) TestDuplicateColumnName(c *C) {
	_, err := parseQuery("SELECT id AS `:id`, f1 AS `f:attr_uint`, f2 AS `f:attr_uint` FROM test")
	c.Assert(err.Error(), Equals, "duplicate column name 'f'")
}

func (s *configTestSuite) TestMultipleIdColumns(c *C) {
	_, err := parseQuery("SELECT id + 1 AS `id1:id`, id * 2 AS `id2:id` FROM test")
	c.Assert(err.Error(), Equals, "columns 'id * 2 as id2' and 'id + 1' cannot both have 'id' type")
}

func (s *configTestSuite) TestNoIdColumn(c *C) {
	_, err := parseQuery("SELECT id + 1 AS `id1:attr_uint`, id * 2 AS `id2:attr_uint` FROM test")
	c.Assert(err.Error(), Equals, "there should be exactly one column with 'id' type, but none found")
}

func Test_buildIndexingQueries(t *testing.T) {
	m := IndexMysqlSettings{
		Host:    "db1",
		Port:    3306,
		User:    "root",
		Pass:    "qwerty",
		Charset: "utf8",
	}
	i := IndexerConfig{Tokenizer: "morphology = stem_en"}
	fields := []IndexConfigField{
		{Name: "id", Type: "id"},
		{Name: "id1", Type: "attr_uint"},
	}
	type args struct {
		m     IndexMysqlSettings
		index string
		cfg   SourceConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []IndexSettings
		wantErr bool
	}{
		{
			name: "one chunk",
			args: args{
				m:     m,
				index: "test_index",
				cfg: SourceConfig{
					Query:   "SELECT t.id AS `:id`, id + 1 AS `id1:attr_uint` FROM test t",
					Parts:   1,
					Indexer: i,
				},
			},
			want: []IndexSettings{{
				Name:    "test_index_plain",
				RTName:  "test_index",
				Fields:  fields,
				Mysql:   m,
				Query:   "select t.id as id, id + 1 as id1 from test as t",
				Indexer: i,
			}},
			wantErr: false,
		},
		{
			name: "many chunks",
			args: args{
				m:     m,
				index: "test_index",
				cfg: SourceConfig{
					Query:   "SELECT t.id AS `:id`, id + 1 AS `id1:attr_uint` FROM test t WHERE t.q < 1 LIMIT 10000",
					Parts:   5,
					Indexer: i,
				},
			},
			want: []IndexSettings{
				{
					Name:    "test_index_plain_part_0",
					RTName:  "test_index_part_0",
					Fields:  fields,
					Mysql:   m,
					Query:   "select t.id as id, id + 1 as id1 from test as t where t.q < 1 and t.id % 5 = 0 limit 10000",
					Indexer: i,
				},
				{
					Name:    "test_index_plain_part_1",
					RTName:  "test_index_part_1",
					Fields:  fields,
					Mysql:   m,
					Query:   "select t.id as id, id + 1 as id1 from test as t where t.q < 1 and t.id % 5 = 1 limit 10000",
					Indexer: i,
				},
				{
					Name:    "test_index_plain_part_2",
					RTName:  "test_index_part_2",
					Fields:  fields,
					Mysql:   m,
					Query:   "select t.id as id, id + 1 as id1 from test as t where t.q < 1 and t.id % 5 = 2 limit 10000",
					Indexer: i,
				},
				{
					Name:    "test_index_plain_part_3",
					RTName:  "test_index_part_3",
					Fields:  fields,
					Mysql:   m,
					Query:   "select t.id as id, id + 1 as id1 from test as t where t.q < 1 and t.id % 5 = 3 limit 10000",
					Indexer: i,
				},
				{
					Name:    "test_index_plain_part_4",
					RTName:  "test_index_part_4",
					Fields:  fields,
					Mysql:   m,
					Query:   "select t.id as id, id + 1 as id1 from test as t where t.q < 1 and t.id % 5 = 4 limit 10000",
					Indexer: i,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildIndexerSettings(tt.args.m, tt.args.index, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildIndexingQueries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildIndexingQueries() = %v, want %v", got, tt.want)
			}
		})
	}
}
