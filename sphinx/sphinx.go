package sphinx

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/superjobru/go-mysql-sphinx/util"
	"gopkg.in/birkirb/loggers.v1/log"
)

// Conn dumb wrapper
type Conn struct {
	*client.Conn
}

// ConnSettings various settings for the connection
type ConnSettings struct {
	DisconnectRetryDelay time.Duration
	OverloadRetryDelay   time.Duration
}

// SphConn struct that holds channels for communicationg with connection goroutines
type SphConn struct {
	conn     *Conn
	hostname string
	in       chan interface{}
	out      chan SphResult
	settings ConnSettings
}

// SphResult no tuples in golang :(
type SphResult struct {
	Result *mysql.Result
	Err    error
}

type query struct {
	stmt string
}

type reloadIndex struct {
	index string
	parts uint16
}

type assertReady struct {
	index string
	parts uint16
}

type optimizeCheck struct {
	index    string
	parts    uint16
	settings IndexMaintenanceSettings
}

type saveSyncState struct {
	state SyncState
}

type loadSyncState struct{}

// IndexMaintenanceSettings ...
type IndexMaintenanceSettings struct {
	MaxRAMChunkBytes uint64
}

// IndexStatus result of 'SHOW INDEX ... STATUS' query
type IndexStatus struct {
	IndexType        string
	IndexedDocuments uint64
	IndexedBytes     uint64
	RAMBytes         uint64
	DiskBytes        uint64
	RAMChunkBytes    uint64
	DiskChunks       uint64
	MemLimit         uint64
}

// SyncState wrapper for the convenience
type SyncState struct {
	Position mysql.Position
	GTID     mysql.GTIDSet
	Flavor   string
}

var errIndexNotReady = errors.New("index is not ready and needs a rebuild")

var errInconsistentClusterState = errors.New("synced state across searchd backends differs")

// Connect constructor wrapper
func Connect(addr string) (*Conn, error) {
	conn, err := client.Connect(addr, "", "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Conn{conn}, nil
}

// ConnectMany connects to a list of sphinx addresses and starts respective goroutines
func ConnectMany(addrList []string, settings ConnSettings, wg *sync.WaitGroup) ([]*SphConn, error) {
	connList := make([]*SphConn, len(addrList))
	for i, addr := range addrList {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		conn, err := Connect(addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		connList[i] = &SphConn{
			conn:     conn,
			hostname: host,
			in:       make(chan interface{}),
			out:      make(chan SphResult),
			settings: settings,
		}
		if wg != nil {
			wg.Add(1)
		}
		go connList[i].startExecLoop(wg)
	}
	return connList, nil
}

// CloseMany closes a bunch of sphinx connections
func CloseMany(sph []*SphConn) {
	for _, conn := range sph {
		conn.Close()
	}
}

func (c *SphConn) startExecLoop(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer c.conn.Close()
	defer close(c.out)
	for req := range c.in {
		var result *mysql.Result
		var err error
		switch r := req.(type) {
		case query:
			result, err = c.execute(r.stmt)
		case reloadIndex:
			err = c.reloadRtIndex(r.index, r.parts)
		case assertReady:
			err = c.assertIndexIsReady(r.index, r.parts)
		case saveSyncState:
			result, err = c.savePosition(r.state)
		case loadSyncState:
			result, err = c.loadPosition()
		case optimizeCheck:
			err = c.checkIndexForOptimize(r.index, r.parts, r.settings)
		default:
			err = errors.Errorf("unknown request type: %T", req)
		}
		c.out <- SphResult{
			Result: result,
			Err:    err,
		}
	}
}

func (c *SphConn) canRetry(err error) (bool, time.Duration) {
	if mysql.ErrorEqual(err, mysql.ErrBadConn) {
		return true, c.settings.DisconnectRetryDelay
	}
	// Unfortunately, sphinx is unable to properly report a "too many connections" error.
	// Its attempt to do that leads to a protocol-level error.
	if strings.HasPrefix(errors.Cause(err).Error(), "invalid sequence ") {
		return true, c.settings.OverloadRetryDelay
	}
	return false, 0
}

func (c *SphConn) execute(stmt string) (*mysql.Result, error) {
	addr := c.RemoteAddr().String()
	log.Infof("[sphinx-query@%s] %s", addr, stmt)
	res, err := c.conn.Conn.Execute(stmt)
	if err != nil {
		log.Errorf("[sphinx-query@%s] [error] %s", addr, err)
		if canRetry, pause := c.canRetry(err); canRetry {
			res, err = c.retry(pause, stmt)
		}
	} else {
		if strings.HasPrefix(strings.ToUpper(stmt), "SELECT") {
			log.Infof("[sphinx-query@%s] [done] selected %d row(s)", addr, res.RowNumber())
		} else {
			log.Infof("[sphinx-query@%s] [done] %d affected row(s)", addr, res.AffectedRows)
		}
	}
	return res, err
}

func (c *SphConn) retry(pause time.Duration, stmt string) (*mysql.Result, error) {
	addr := c.RemoteAddr().String()
	err := c.conn.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[sphinx-query@%s] waiting for %s before a retry", addr, pause)
	time.Sleep(pause)
	err = c.reconnect()
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[sphinx-query@%s] [retry] %s", addr, stmt)
	return c.conn.Conn.Execute(stmt)
}

// Hostname as specified in the configuration file
// used for the integration with HAProxy
func (c *SphConn) Hostname() string {
	return c.hostname
}

// LocalAddr returns the local network address
func (c *SphConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (c *SphConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ReloadRtIndex truncates rt-index, then attaches a plain index
// Actual implementation is the `reloadRtIndex` method
func (c *SphConn) ReloadRtIndex(rtIndex string, parts uint16) error {
	c.in <- reloadIndex{index: rtIndex, parts: parts}
	res := <-c.out
	return res.Err
}

// AssertIndexIsReady checks if given index is ready to accept queries
// Actual implementation is the `assertIndexIsReady` method
func (c *SphConn) AssertIndexIsReady(rtIndex string, parts uint16) error {
	c.in <- assertReady{index: rtIndex, parts: parts}
	res := <-c.out
	return res.Err
}

// Close closes connection to sphinx
// after closing reusing that connection is impossible
func (c *SphConn) Close() {
	close(c.in)
}

// Reconnect tries to reconnect
func (c *SphConn) reconnect() error {
	addr := c.RemoteAddr().String()
	log.Infof("[sphinx-reconnect] %s", addr)
	conn, err := client.Connect(addr, "", "", "")
	if err != nil {
		return errors.Trace(err)
	}
	c.conn.Conn = conn
	return nil
}

// reloadRtIndex handler counterpart of `ReloadRtIndex` method
func (c *SphConn) reloadRtIndex(rtIndex string, parts uint16) error {
	var err error
	var chunkNo uint16
	plainIndex := util.PlainIndexName(rtIndex)
	for chunkNo = 0; chunkNo < parts; chunkNo++ {
		rtIndexChunk := util.IndexChunkName(rtIndex, parts, chunkNo)
		plainIndexChunk := util.IndexChunkName(plainIndex, parts, chunkNo)
		err = c.reloadRtIndexChunk(rtIndexChunk, plainIndexChunk)
		if err != nil {
			return errors.Annotatef(err, "failed to reload index '%s' (chunk #%d)", rtIndex, chunkNo)
		}
	}
	return nil
}

// reloadRtIndexChunk reloads an index chunk
func (c *SphConn) reloadRtIndexChunk(rtIndex string, plainIndex string) error {
	err := c.reloadPlainIndex(plainIndex)
	if err != nil {
		return errors.Trace(err)
	}
	return c.truncateWithAttach(plainIndex, rtIndex)
}

func (c *SphConn) checkIndexForOptimize(rtIndex string, parts uint16, s IndexMaintenanceSettings) error {
	var err error
	var chunkNo uint16
	for chunkNo = 0; chunkNo < parts; chunkNo++ {
		rtIndexChunk := util.IndexChunkName(rtIndex, parts, chunkNo)
		err = c.checkIndexChunkForOptimize(rtIndexChunk, s)
		if err != nil {
			return errors.Annotatef(err, "could not optimize index '%s' (chunk #%d)", rtIndex, chunkNo)
		}
	}
	return nil
}

func (c *SphConn) checkIndexChunkForOptimize(rtIndex string, s IndexMaintenanceSettings) error {
	status, err := c.getIndexChunkStatus(rtIndex)
	if err != nil {
		return errors.Annotatef(err, "could not get status for %s", rtIndex)
	}
	addr := c.RemoteAddr().String()
	statusStr, _ := json.Marshal(status)
	log.Infof("[maintenance-check@%s] %s status: %s", addr, rtIndex, statusStr)
	if status.RAMChunkBytes > s.MaxRAMChunkBytes && s.MaxRAMChunkBytes > 0 {
		return c.doIndexChunkOptimize(rtIndex)
	}
	return nil
}

func (c *SphConn) doIndexChunkOptimize(rtIndex string) error {
	var err error
	flushQuery := fmt.Sprintf("FLUSH RAMCHUNK %s", rtIndex)
	_, err = c.execute(flushQuery)
	if err != nil {
		return errors.Trace(err)
	}
	optimizeQuery := fmt.Sprintf("OPTIMIZE INDEX %s", rtIndex)
	_, err = c.execute(optimizeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *SphConn) getIndexChunkStatus(index string) (*IndexStatus, error) {
	var err error
	query := fmt.Sprintf("SHOW INDEX %s STATUS", index)
	result, err := c.execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	status := &IndexStatus{}
	for i := 0; i < result.RowNumber(); i++ {
		if err = decodeStatusRow(result, i, status); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return status, nil
}

func decodeStatusRow(result *mysql.Result, rowNo int, status *IndexStatus) error {
	varName, err := result.GetString(rowNo, 0)
	if err != nil {
		return errors.Annotatef(err, "could not read variable name")
	}
	switch varName {
	case "index_type":
		status.IndexType, err = result.GetString(rowNo, 1)
	case "indexed_documents":
		status.IndexedDocuments, err = result.GetUint(rowNo, 1)
	case "indexed_bytes":
		status.IndexedBytes, err = result.GetUint(rowNo, 1)
	case "ram_bytes":
		status.RAMBytes, err = result.GetUint(rowNo, 1)
	case "disk_bytes":
		status.DiskBytes, err = result.GetUint(rowNo, 1)
	case "ram_chunk":
		status.RAMChunkBytes, err = result.GetUint(rowNo, 1)
	case "disk_chunks":
		status.DiskChunks, err = result.GetUint(rowNo, 1)
	case "mem_limit":
		status.MemLimit, err = result.GetUint(rowNo, 1)
	}
	if err != nil {
		return errors.Annotatef(err, "could not decode value of %s", varName)
	}
	return nil
}

// IndexExists checks if searchd serves an index
func (c *SphConn) IndexExists(index string) (bool, error) {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", index)
	res, err := c.execute(query)
	if err != nil {
		return false, errors.Annotatef(err, "could not check if %s index exists", index)
	}
	return res.RowNumber() > 0, nil
}

// assertIndexIsReady handler counterpart of `AssertIndexIsReady` method
func (c *SphConn) assertIndexIsReady(index string, parts uint16) error {
	var chunkNo uint16
	for chunkNo = 0; chunkNo < parts; chunkNo++ {
		indexChunk := util.IndexChunkName(index, parts, chunkNo)
		isEmpty, err := c.IndexChunkIsUninitialized(indexChunk)
		if err != nil {
			return errors.Annotatef(err, "failed to reload index '%s' (chunk #%d)", index, chunkNo)
		}
		if isEmpty {
			return errors.Annotatef(errIndexNotReady, "index chunk %s is not ready", indexChunk)
		}
	}
	return nil
}

// IndexChunkIsUninitialized checks if given index chunk is not initialized
// For now it is done by convention: uninitialized index contains no fields and attributes
// except two: "dummy_field" and "dummy_attr"
func (c *SphConn) IndexChunkIsUninitialized(index string) (bool, error) {
	query := fmt.Sprintf("DESC %s", index)
	res, err := c.execute(query)
	dummySet := set.NewSetWith("id", "dummy_field", "dummy_attr")
	fieldSet := set.NewSet()
	if err == nil {
		for i := 0; i < 3; i++ {
			var columnName string
			columnName, err = res.GetString(i, 0)
			if err != nil {
				break
			} else {
				fieldSet.Add(columnName)
			}
		}
	}
	if err != nil {
		return false, errors.Annotatef(err, "could not check if %s index is empty", index)
	}
	return fieldSet.Equal(dummySet), nil
}

// reloadPlainIndex reloads a plain index
// this should be done right before attaching it to rt-index
func (c *SphConn) reloadPlainIndex(index string) error {
	query := fmt.Sprintf("RELOAD INDEX %s", index)
	_, err := c.execute(query)
	return err
}

// truncateWithAttach truncates rt-index, then attaches a plain index
func (c *SphConn) truncateWithAttach(plainIndex string, rtIndex string) error {
	var err error
	truncateQuery := fmt.Sprintf("TRUNCATE RTINDEX %s", rtIndex)
	_, err = c.execute(truncateQuery)
	if err != nil {
		return errors.Trace(err)
	}
	attachQuery := fmt.Sprintf("ATTACH INDEX %s TO RTINDEX %s", plainIndex, rtIndex)
	_, err = c.execute(attachQuery)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *SphConn) savePosition(p SyncState) (*mysql.Result, error) {
	query := fmt.Sprintf(
		"REPLACE INTO sync_state (id, binlog_name, binlog_position, gtid, flavor) VALUES (1, %s, %d, %s, %s)",
		QuoteString(p.Position.Name),
		p.Position.Pos,
		QuoteString(p.GTID.String()),
		QuoteString(p.Flavor),
	)
	return c.execute(query)
}

func (c *SphConn) loadPosition() (*mysql.Result, error) {
	res, err := c.execute("SELECT binlog_name, binlog_position, gtid, flavor FROM sync_state WHERE id = 1")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if res.RowNumber() < 1 {
		return nil, nil
	}
	return res, nil
}

// ExecuteInParallel executes the same query for each sphinx connection
func ExecuteInParallel(sph []*SphConn, stmt string) []SphResult {
	result := make([]SphResult, len(sph))
	for _, conn := range sph {
		conn.in <- query{stmt: stmt}
	}
	for i, conn := range sph {
		result[i] = <-conn.out
	}
	return result
}

// IndexIsReady checks index readiness across all specified sphinx connections
func IndexIsReady(sph []*SphConn, index string, parts uint16) (bool, error) {
	result := make([]SphResult, len(sph))
	for _, conn := range sph {
		conn.in <- assertReady{index: index, parts: parts}
	}
	for i, conn := range sph {
		result[i] = <-conn.out
	}
	for _, res := range result {
		if res.Err == nil {
			continue
		}
		if errors.Cause(res.Err) == errIndexNotReady {
			return false, nil
		}
		return false, errors.Trace(res.Err)
	}
	return true, nil
}

// CheckIndexForOptimize checks if index maintenance is needed and performs it accordingly
func CheckIndexForOptimize(sph []*SphConn, index string, parts uint16, s IndexMaintenanceSettings) error {
	result := make([]SphResult, len(sph))
	for _, conn := range sph {
		conn.in <- optimizeCheck{index: index, parts: parts, settings: s}
	}
	for i, conn := range sph {
		result[i] = <-conn.out
	}
	for _, res := range result {
		if res.Err != nil {
			return errors.Trace(res.Err)
		}
	}
	return nil
}

// SaveSyncState saves the synchronization state to the cluster
func SaveSyncState(sph []*SphConn, state SyncState) error {
	result := make([]SphResult, len(sph))
	for _, conn := range sph {
		conn.in <- saveSyncState{state}
	}
	for i, conn := range sph {
		result[i] = <-conn.out
	}
	for _, res := range result {
		if res.Err != nil {
			return errors.Trace(res.Err)
		}
	}
	return nil
}

// LoadSyncState checks if the saved state across sphinx backends is the same
// If no sphinx backend has any saved state, then returns nil
// If any two backends have different state, then returns errInconsistentClusterState
func LoadSyncState(sph []*SphConn, masterState SyncState) (state *SyncState, err error) {
	result := make([]SphResult, len(sph))
	for _, conn := range sph {
		conn.in <- loadSyncState{}
	}
	for i, conn := range sph {
		result[i] = <-conn.out
	}
	stateList := []*SyncState{}
	for _, res := range result {
		if res.Err != nil {
			err = errors.Trace(res.Err)
			return
		}

		state, err = newSyncState(res.Result)
		if err != nil {
			err = errors.Trace(res.Err)
			return
		}

		stateList = append(stateList, state)
	}
	for i := range stateList {
		state = stateList[i]
		if i > 0 && !reflect.DeepEqual(state, stateList[i-1]) {
			state0, _ := json.Marshal(stateList[i-1])
			state1, _ := json.Marshal(state)
			return nil, errors.Annotatef(
				errInconsistentClusterState,
				"backend #%d: %s; backend #%d: %s",
				i-1,
				state0,
				i,
				state1,
			)
		}
	}
	return
}

// QuoteString is the same as mysql.Escape, but with handling of sphinx quirks
func QuoteString(val string) string {
	val = strings.Replace(val, "\\", "\\\\", -1)
	val = strings.Replace(val, "'", "\\'", -1)
	val = strings.Replace(val, "\x00", "", -1)
	return fmt.Sprintf("'%s'", val)
}

func newSyncState(res *mysql.Result) (*SyncState, error) {
	if res == nil {
		return nil, nil
	}
	binlogName, err := res.GetStringByName(0, "binlog_name")
	if err != nil {
		return nil, errors.Trace(err)
	}
	binlogPosition, err := res.GetUintByName(0, "binlog_position")
	if err != nil {
		return nil, errors.Trace(err)
	}
	gtidStr, err := res.GetStringByName(0, "gtid")
	if err != nil {
		return nil, errors.Trace(err)
	}
	flavor, err := res.GetStringByName(0, "flavor")
	if err != nil {
		return nil, errors.Trace(err)
	}
	gtid, err := mysql.ParseGTIDSet(flavor, gtidStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &SyncState{
		Position: mysql.Position{
			Name: binlogName,
			Pos:  uint32(binlogPosition),
		},
		GTID:   gtid,
		Flavor: flavor,
	}, nil
}
