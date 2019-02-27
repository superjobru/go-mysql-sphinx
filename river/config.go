package river

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	set "github.com/deckarep/golang-set"
	"github.com/juju/errors"
	"github.com/superjobru/go-mysql-sphinx/util"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

// IndexerConfig is the configs for source
type IndexerConfig struct {
	MemLimit           string            `toml:"mem_limit"`
	MaxIops            string            `toml:"max_iops"`
	MaxIosize          string            `toml:"max_iosize"`
	MaxXmlpipe2Field   string            `toml:"max_xmlpipe2_field"`
	WriteBuffer        string            `toml:"write_buffer"`
	MaxFileFieldBuffer string            `toml:"max_file_field_buffer"`
	OnFileFieldError   string            `toml:"on_file_field_error"`
	LemmatizerCache    string            `toml:"lemmatizer_cache"`
	GroupConcatMaxLen  uint64            `toml:"group_concat_max_len"`
	Tokenizer          string            `toml:"tokenizer"`
	Dictionaries       IndexDictionaries `toml:"dictionaries"`
}

// IndexUploaderConfig ...
type IndexUploaderConfig struct {
	Executable string            `toml:"executable"`
	Arguments  []TomlGoTemplate  `toml:"arguments"`
	HostMap    map[string]string `toml:"host_map"`
}

// IndexMaintenanceConfig ...
type IndexMaintenanceConfig struct {
	OptimizeSchedule string `toml:"optimize_schedule"`
	RebuildSchedule  string `toml:"rebuild_schedule"`
	MaxRAMChunkBytes uint64 `toml:"max_ram_chunk_bytes"`
}

// SourceConfig contains all info that's needed to build or update an index
type SourceConfig struct {
	Indexer     IndexerConfig `toml:"indexer"`
	Query       string        `toml:"query"`
	Parts       uint16        `toml:"parts"`
	StoragePath string        `toml:"storage_path"`
	details     *SourceConfigDetails
}

// SourceConfigDetails ...
type SourceConfigDetails struct {
	attrFields   set.Set
	docIDExpr    *sqlparser.Expr
	fieldList    []IndexConfigField
	fieldTypes   map[string]string
	queryColumns map[string]set.Set
	queryTables  map[string]string
	queryAst     *sqlparser.Select
	queryTpl     *sqlparser.ParsedQuery
}

// SphConnSettings various settings for the connection to sphinx
type SphConnSettings struct {
	DisconnectRetryDelay TomlDuration `toml:"disconnect_retry_delay"`
	OverloadRetryDelay   TomlDuration `toml:"overload_retry_delay"`
}

// BalancerConfig details for connecting to the load balancer
// Used for disabling a server during RELOAD-TRUNCATE-ATTACH sequence
// Only HAProxy is supported
type BalancerConfig struct {
	Addr                []string     `toml:"addr"`
	BackendName         []string     `toml:"backend"`
	PauseAfterDisabling TomlDuration `toml:"pause_after_disabling"`
	PauseBeforeEnabling TomlDuration `toml:"pause_before_enabling"`
	Timeout             TomlDuration `toml:"timeout"`
	UseTLS              bool         `toml:"use_tls"`
	ServerCAFile        string       `toml:"server_ca_file"`
	ClientCertFile      string       `toml:"client_cert_file"`
	ClientKeyFile       string       `toml:"client_key_file"`
	TLSServerName       string       `toml:"tls_server_name"`
}

const (
	// DocID document id
	DocID = "id"
	// AttrFloat rt_attr_float
	AttrFloat = "attr_float"
	// AttrUint rt_attr_uint
	AttrUint = "attr_uint"
	// AttrBigint rt_attr_bigint
	AttrBigint = "attr_bigint"
	// AttrMulti rt_attr_multi
	AttrMulti = "attr_multi"
	// AttrMulti64 rt_attr_multi_64
	AttrMulti64 = "attr_multi_64"
	// AttrString rt_attr_string
	AttrString = "attr_string"
	// TextField rt_field
	TextField = "field"
)

// Config is the configuration
type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`
	MyCharset  string `toml:"my_charset"`

	SphAddr []string `toml:"sph_addr"`

	Balancer BalancerConfig `toml:"balancer"`

	MaintenanceConfig IndexMaintenanceConfig `toml:"maintenance"`

	IndexUploader IndexUploaderConfig `toml:"index_uploader"`

	SphConnSettings SphConnSettings `toml:"sph_conn_settings"`

	StatAddr string `toml:"stat_addr"`

	ServerID        uint32       `toml:"server_id"`
	Flavor          string       `toml:"flavor"`
	HeartbeatPeriod TomlDuration `toml:"heartbeat_period"`
	DataDir         string       `toml:"data_dir"`

	DumpExec       string `toml:"mysqldump"`
	SkipMasterData bool   `toml:"skip_master_data"`

	SkipSphSyncState bool `toml:"skip_sph_sync_state"`

	IngestRules []IngestRule `toml:"ingest"`

	DataSource map[string]*SourceConfig `toml:"data_source"`

	FlushBulkTime TomlDuration `toml:"flush_bulk_time"`

	MinTimeAfterLastEvent  TomlDuration `toml:"min_time_after_last_event"`
	MaxTimeAfterFirstEvent TomlDuration `toml:"max_time_after_first_event"`

	ReplayFullBinlog bool `toml:"replay_full_binlog"`

	UseGTID bool `toml:"use_gtid"`
}

// IndexMysqlSettings settings for connection to mysql from indexer
type IndexMysqlSettings struct {
	Host    string
	Port    uint16
	User    string
	Pass    string
	Charset string
}

// IndexConfigField field of an index as it's seen in the indexer config
type IndexConfigField struct {
	Name string
	Type string
}

// IndexDictionaries file path for each of the dictionary file
type IndexDictionaries struct {
	Exceptions string
	Stopwords  string
	Wordforms  string
}

// IndexSettings struct for templating the indexer config
type IndexSettings struct {
	Name    string
	RTName  string
	Indexer IndexerConfig
	Mysql   IndexMysqlSettings
	Fields  []IndexConfigField
	Query   string
}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func splitAddr(addr string, defaultPort int) (string, uint16, error) {
	var host string
	var port int
	var err error
	addrParts := strings.Split(addr, ":")
	if addrParts[0] == "" {
		return "", 0, errors.Errorf("address must be non-empty")
	}
	host = addrParts[0]
	if len(addrParts) > 1 {
		port, err = strconv.Atoi(addrParts[1])
		if err != nil {
			return "", 0, errors.Annotatef(err, "port must be an integer, got %v instead", addrParts[1])
		}
	} else {
		port = defaultPort
	}
	return host, uint16(port), nil
}

func newIndexerMysqlSettings(c *Config) (*IndexMysqlSettings, error) {
	host, port, err := splitAddr(c.MyAddr, 3306)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid mysql address: %s", c.MyAddr)
	}

	return &IndexMysqlSettings{
		Host:    host,
		Port:    port,
		User:    c.MyUser,
		Pass:    c.MyPassword,
		Charset: c.MyCharset,
	}, nil
}

// NewConfig creates a Config from data.
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.MyUser = os.ExpandEnv(c.MyUser)
	c.MyPassword = os.ExpandEnv(c.MyPassword)
	c.DataDir = os.ExpandEnv(c.DataDir)

	if c.IndexUploader.Executable == "" {
		c.IndexUploader.Executable = "rsync"
	}
	for _, arg := range c.IndexUploader.Arguments {
		if _, err = expandUploaderArg(arg, UploaderArgSubst{}); err != nil {
			return nil, errors.Annotate(err, "invalid index_uploader.arguments")
		}
	}

	for index, cfg := range c.DataSource {
		if cfg.Parts < 1 {
			cfg.Parts = 1
		}
		if cfg.StoragePath == "" {
			cfg.StoragePath = filepath.Join(c.DataDir, "index-storage", index)
		}
		cfg.details, err = parseQuery(cfg.Query)
		if err != nil {
			return nil, errors.Annotatef(err, "invalid query for index '%s'", index)
		}
	}

	for idx, rule := range c.IngestRules {
		c.IngestRules[idx].timeProvider = time.Now
		if err = rule.check(c.DataSource); err != nil {
			return nil, errors.Annotatef(err, "error in rule #%d for table '%s'", idx, rule.TableName)
		}
	}

	c.applyDefaults()

	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.MaxTimeAfterFirstEvent.Duration == 0 {
		c.MaxTimeAfterFirstEvent = TomlDuration{Duration: 10 * time.Second}
	}
	if c.SphConnSettings.DisconnectRetryDelay.Duration.Nanoseconds() == 0 {
		c.SphConnSettings.DisconnectRetryDelay = TomlDuration{Duration: time.Second}
	}
	if c.SphConnSettings.OverloadRetryDelay.Duration.Nanoseconds() == 0 {
		c.SphConnSettings.OverloadRetryDelay = TomlDuration{Duration: time.Minute}
	}
}

func (s *SourceConfig) hasTable(tableName string) bool {
	for _, t := range s.details.queryTables {
		if t == tableName {
			return true
		}
	}
	return false
}

func checkIndexColumnType(t string) error {
	switch t {
	case
		DocID,
		AttrFloat,
		AttrUint,
		AttrBigint,
		AttrMulti,
		AttrMulti64,
		AttrString,
		TextField:
		return nil
	default:
		return errors.Errorf("invalid index column type: %s", t)
	}
}

func parseSelectQuery(query string) (*SourceConfigDetails, error) {
	var queryAst *sqlparser.Select

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to parse SQL query")
	}
	queryAst, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.Errorf("expected a SELECT query, but got %T", stmt)
	}

	if queryHasNoFromClause(queryAst) {
		return nil, errors.Errorf("SQL query must have a FROM clause")
	}

	cfg, err := parseSelectExpressions(queryAst)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}

func parseQuery(query string) (*SourceConfigDetails, error) {

	cfg, err := parseSelectQuery(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	queryAst := cfg.queryAst

	queryAst.AddWhere(
		&sqlparser.ComparisonExpr{
			Operator: "in",
			Left:     *cfg.docIDExpr,
			Right:    sqlparser.ListArg("::doc_id_condition"),
		},
	)

	cfg.queryTables, err = tableMapFromSelectQuery(queryAst)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.queryColumns, err = columnSetFromSelectQuery(queryAst, cfg.queryTables)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.queryTpl = sqlparser.NewParsedQuery(queryAst)

	return cfg, nil
}

func queryHasNoFromClause(queryAst *sqlparser.Select) bool {
	if len(queryAst.From) == 1 {
		switch f := queryAst.From[0].(type) {
		case *sqlparser.AliasedTableExpr:
			switch e := f.Expr.(type) {
			case sqlparser.TableName:
				if e.Name.String() == "dual" {
					return true
				}
			}
		}
	}
	return false
}

func parseSelectExpressions(queryAst *sqlparser.Select) (*SourceConfigDetails, error) {
	cfg := &SourceConfigDetails{
		attrFields: set.NewSet(),
		fieldList:  make([]IndexConfigField, len(queryAst.SelectExprs)),
		fieldTypes: make(map[string]string),
		queryAst:   queryAst,
	}

	for i, expr := range queryAst.SelectExprs {
		var fieldExpr *sqlparser.AliasedExpr
		switch se := expr.(type) {
		case *sqlparser.AliasedExpr:
			fieldExpr = se
		default:
			return nil, errors.Errorf("expected an aliased expression, but got %T", expr)
		}
		colName, colType, err := parseSelectColumn(fieldExpr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if _, colExists := cfg.fieldTypes[colName]; colExists {
			return nil, errors.Errorf("duplicate column name '%s'", colName)
		}
		cfg.fieldList[i] = IndexConfigField{
			Name: colName,
			Type: colType,
		}
		cfg.fieldTypes[colName] = colType
		fieldExpr.As = sqlparser.NewColIdent(colName)
		if colType == DocID {
			if cfg.docIDExpr != nil {
				return nil, errors.Errorf("columns '%s' and '%s' cannot both have 'id' type", sqlExprToString(fieldExpr), sqlExprToString(*cfg.docIDExpr))
			}
			cfg.docIDExpr = &fieldExpr.Expr
		}
		switch colType {
		case
			AttrFloat,
			AttrUint,
			AttrBigint,
			AttrMulti,
			AttrMulti64:
			cfg.attrFields.Add(colName)
		}
	}
	if cfg.docIDExpr == nil {
		return nil, errors.Errorf("there should be exactly one column with 'id' type, but none found")
	}
	return cfg, nil
}

func parseSelectColumn(fieldExpr *sqlparser.AliasedExpr) (string, string, error) {
	alias := fieldExpr.As.String()
	if fieldExpr == nil || alias == "" {
		return "", "", errors.Errorf("select expression '%s' must have an alias", sqlExprToString(fieldExpr))
	}
	splitAlias := strings.Split(alias, ":")
	if len(splitAlias) != 2 || splitAlias[1] == "" {
		return "", "", errors.Errorf("alias '%s' in expression '%s' must conform to '[ColumnName]:ColumnType' format", alias, sqlExprToString(fieldExpr))
	}
	var colName string
	column, isBareColumn := fieldExpr.Expr.(*sqlparser.ColName)
	if splitAlias[0] == "" {
		if isBareColumn {
			colName = column.Name.String()
		} else {
			return "", "", errors.Errorf("alias '%s' in expression '%s' must conform to 'ColumnName:ColumnType' format (since the aliased expression is not a column name)", alias, sqlExprToString(fieldExpr))
		}
	} else {
		colName = splitAlias[0]
	}
	colType := splitAlias[1]
	if err := checkIndexColumnType(colType); err != nil {
		return "", "", errors.Annotatef(err, "invalid column alias '%s'", alias)
	}
	return colName, colType, nil
}

func buildSelectQuery(queryTpl *sqlparser.ParsedQuery, ids []uint64) (string, error) {
	bindVar, err := sqltypes.BuildBindVariable(ids)
	if err != nil {
		return "", errors.Trace(err)
	}

	queryBuf, err := queryTpl.GenerateQuery(
		map[string]*querypb.BindVariable{"doc_id_condition": bindVar},
		make(map[string]sqlparser.Encodable),
	)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(queryBuf), nil
}

func buildIndexingQuery(tpl *sqlparser.ParsedQuery, parts, chunkNo uint16) (string, error) {
	partsVar, err := sqltypes.BuildBindVariable(uint64(parts))
	if err != nil {
		return "", errors.Trace(err)
	}
	chunkVar, err := sqltypes.BuildBindVariable(uint64(chunkNo))
	if err != nil {
		return "", errors.Trace(err)
	}
	query, err := tpl.GenerateQuery(
		map[string]*querypb.BindVariable{
			"parts": partsVar,
			"chunk": chunkVar,
		},
		make(map[string]sqlparser.Encodable),
	)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(query), nil
}

func buildIndexerSettings(m IndexMysqlSettings, index string, c SourceConfig) ([]IndexSettings, error) {
	details, err := parseSelectQuery(c.Query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if c.Parts > 1 {
		details.queryAst.AddWhere(
			&sqlparser.ComparisonExpr{
				Operator: "=",
				Left: &sqlparser.BinaryExpr{
					Operator: "%",
					Left:     *details.docIDExpr,
					Right:    sqlparser.ListArg(":parts"),
				},
				Right: sqlparser.ListArg(":chunk"),
			},
		)
	}
	queryTpl := sqlparser.NewParsedQuery(details.queryAst)
	settings := make([]IndexSettings, c.Parts)
	var chunkNo uint16
	for chunkNo = 0; chunkNo < c.Parts; chunkNo++ {
		query, err := buildIndexingQuery(queryTpl, c.Parts, chunkNo)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to build indexing query (index %s chunk %d)", index, chunkNo)
		}
		settings[chunkNo] = IndexSettings{
			Name:    util.IndexChunkName(util.PlainIndexName(index), c.Parts, chunkNo),
			RTName:  util.IndexChunkName(index, c.Parts, chunkNo),
			Fields:  details.fieldList,
			Mysql:   m,
			Query:   query,
			Indexer: c.Indexer,
		}
	}
	return settings, nil
}

func sqlExprToString(e sqlparser.SQLNode) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	e.Format(buf)
	return buf.String()
}

func tableMapFromSelectQuery(e *sqlparser.Select) (map[string]string, error) {
	tableMap := map[string]string{}

	tableVisitor := func(n sqlparser.SQLNode) (bool, error) {
		switch t := n.(type) {
		case sqlparser.JoinCondition:
			return false, nil
		case *sqlparser.AliasedTableExpr:
			tName, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				return false, errors.Errorf("expected TableName, but got %s", spew.Sdump(t.Expr))
			}
			alias := t.As.String()
			name := sqlExprToString(tName)
			if alias == "" {
				return false, errors.Errorf("no alias for table '%s'", name)
			}
			otherName, aliasExists := tableMap[alias]
			if aliasExists {
				return false, errors.Errorf("duplicate alias '%s' for tables '%s' and '%s'", alias, otherName, name)
			}
			tableMap[alias] = name
		}
		return true, nil
	}

	err := sqlparser.Walk(tableVisitor, e.From)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tableMap, nil
}

func columnSetFromSelectQuery(e *sqlparser.Select, tableMap map[string]string) (map[string]set.Set, error) {
	var columnMap = map[string]set.Set{}

	columnVisitor := func(n sqlparser.SQLNode) (bool, error) {
		column, isBareColumn := n.(*sqlparser.ColName)
		if isBareColumn {
			tableAlias := column.Qualifier.Name.String()
			if tableAlias == "" {
				return false, errors.Errorf("no table alias in '%s'", sqlExprToString(column))
			}
			tableName, exists := tableMap[tableAlias]
			if !exists {
				return false, errors.Errorf("unknown alias '%s' in '%s'", tableAlias, sqlExprToString(column))
			}
			_, exists = columnMap[tableName]
			if !exists {
				columnMap[tableName] = set.NewSet()
			}
			columnMap[tableName].Add(column.Name.String())
			return false, nil
		}
		return true, nil
	}

	err := sqlparser.Walk(columnVisitor, e.SelectExprs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return columnMap, nil
}
