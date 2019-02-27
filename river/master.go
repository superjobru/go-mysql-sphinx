package river

import (
	"bytes"
	"os"
	"path"
	"sync"
	"time"

	"github.com/superjobru/go-mysql-sphinx/sphinx"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	"gopkg.in/birkirb/loggers.v1/log"
)

type masterState struct {
	sync.RWMutex
	dataDir           string
	filePath          string
	flavor            string
	gtid              *mysql.GTIDSet
	pos               *mysql.Position
	useGTID           bool
	needPositionReset bool
	lastSaveTime      time.Time
}

type positionData struct {
	GTID  string `toml:"gtid"`
	Name  string `toml:"bin_name"`
	Pos   uint32 `toml:"bin_pos"`
	Reset bool   `toml:"reset"`
}

// MysqlPositionProvider used in resetToCurrent() method
type MysqlPositionProvider interface {
	GetMasterGTIDSet() (mysql.GTIDSet, error)
	GetMasterPos() (mysql.Position, error)
}

// newMasterState master state constructor
func newMasterState(c *Config) *masterState {
	var m = &masterState{useGTID: c.UseGTID, flavor: c.Flavor}
	if c.DataDir != "" {
		m.dataDir = c.DataDir
		m.filePath = path.Join(c.DataDir, "master.info")
	}
	return m
}

func (m *masterState) load() (err error) {
	var p positionData

	if m.dataDir == "" {
		log.Info("skipped loading master info: no data dir specified")
		return nil
	}

	m.Lock()
	defer m.Unlock()

	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(m.dataDir, 0755); err != nil {
		return errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		log.Infof("skipped loading master info: %s file not found", m.filePath)
		return nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, &p)
	if err != nil {
		return errors.Trace(err)
	}
	m.needPositionReset = p.Reset
	m.pos = &mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}
	if !m.useGTID {
		return nil
	}
	gtid, err := mysql.ParseGTIDSet(m.flavor, p.GTID)
	if err != nil {
		return errors.Trace(err)
	}
	m.gtid = &gtid
	log.Infof("loaded GTID: %v", *m.gtid)
	return nil
}

func (m *masterState) save() error {
	m.Lock()
	defer m.Unlock()

	if len(m.dataDir) == 0 {
		log.Infof("skipped saving position (no data dir specified)")
		return nil
	}

	var pos mysql.Position
	var gtidStr string
	if m.pos != nil {
		pos = *m.pos
	}
	if m.gtid == nil {
		gtidStr = ""
	} else {
		gtidStr = (*m.gtid).String()
	}
	p := positionData{
		Name:  pos.Name,
		Pos:   pos.Pos,
		GTID:  gtidStr,
		Reset: m.needPositionReset,
	}

	m.lastSaveTime = time.Now()
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(p)

	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("could not save master info to file '%s': %v", m.filePath, err)
	} else {
		log.Debugf("saved master info to file '%s': %v", m.filePath, p)
	}

	return nil
}

func (m *masterState) updatePosition(posEvent positionEvent) bool {
	m.Lock()
	defer m.Unlock()

	var hasChanged bool

	if m.pos == nil {
		hasChanged = true
	} else if m.pos.Compare(posEvent.pos) != 0 {
		hasChanged = true
	}

	m.pos = &mysql.Position{
		Name: posEvent.pos.Name,
		Pos:  posEvent.pos.Pos,
	}

	if posEvent.gtid != nil {
		if m.gtid == nil {
			hasChanged = true
		} else if !(*m.gtid).Contain(posEvent.gtid) {
			hasChanged = true
		}
		m.gtid = &posEvent.gtid
	}

	return hasChanged
}

func (m *masterState) gtidSet() mysql.GTIDSet {
	m.Lock()
	defer m.Unlock()

	if m.gtid == nil {
		return nil
	}

	return (*m.gtid).Clone()
}

func (m *masterState) gtidString() string {
	m.Lock()
	defer m.Unlock()

	if m.gtid == nil {
		return ""
	}

	return (*m.gtid).String()
}

func (m *masterState) position() mysql.Position {
	m.Lock()
	defer m.Unlock()

	if m.pos == nil {
		return mysql.Position{}
	}

	return *m.pos
}

func (m *masterState) syncState() sphinx.SyncState {
	m.Lock()
	defer m.Unlock()

	var gtid mysql.GTIDSet
	var pos mysql.Position
	if m.gtid != nil {
		gtid = *m.gtid
	}
	if m.pos != nil {
		pos = *m.pos
	}

	return sphinx.SyncState{
		Position: pos,
		GTID:     gtid,
		Flavor:   m.flavor,
	}
}

func (m *masterState) String() string {
	return spew.Sprintf(
		"filePath=%v gtid=%v pos=%v useGTID=%v needPositionReset=%v",
		m.filePath,
		m.gtid,
		m.pos,
		m.useGTID,
		m.needPositionReset,
	)
}

func (m *masterState) resetToCurrent(p MysqlPositionProvider) error {
	m.Lock()
	defer m.Unlock()

	if m.useGTID {
		currentGTID, err := p.GetMasterGTIDSet()
		if err != nil {
			return errors.Trace(err)
		}
		m.gtid = &currentGTID
	}

	currentPos, err := p.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}
	m.pos = &currentPos

	return nil
}
