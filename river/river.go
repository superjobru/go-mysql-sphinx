package river

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/canal"
	"github.com/superjobru/go-mysql-sphinx/sphinx"
	"github.com/thejerf/suture"
	"gopkg.in/birkirb/loggers.v1"
	"gopkg.in/birkirb/loggers.v1/log"
)

// River is (actually, was) a pluggable service within Elasticsearch that pulls data from an external source.
// https://www.elastic.co/blog/the-river
// https://www.elastic.co/blog/deprecating-rivers
// We use this definition here for brevity, although this service obviously does not run within Elasticsearch.
type River struct {
	Log loggers.Contextual

	l loggers.Advanced

	c *Config

	canal *canal.Canal

	ctx    context.Context
	Cancel context.CancelFunc

	sph []*sphinx.SphConn

	balancer *BalancerClient

	sphinxService *SphinxService
	syncService   *SyncService

	StatService *stat

	master *masterState

	rebuildInProgress set.Set

	// protects isRunning flag
	m sync.Mutex

	isRunning bool

	syncC chan interface{}

	done chan struct{}

	FatalErrC chan error

	rebuildAndExit bool

	sup         *suture.Supervisor
	sphinxToken suture.ServiceToken
	cronToken   *suture.ServiceToken
	syncToken   *suture.ServiceToken
	canalToken  *suture.ServiceToken

	syncM sync.Mutex
}

// ErrRebuildAndExitFlagSet this is used in main() to return appropriate exit code
var ErrRebuildAndExitFlagSet = errors.New("rebuild-and-exit option is used, exiting")

var errSphinxDisconnected = errors.New("sphinx connections are already closed")

var errIndexRebuildInProgress = errors.New("cannot do index maintenance while it's rebuilding")

var errWaitForGTIDTimedOut = errors.New("waited for GTID sync for too long")

const canalServiceStopTimeout = 10 * time.Second
const cronServiceStopTimeout = 10 * time.Second
const sphinxServiceStopTimeout = 10 * time.Second
const syncServiceStopTimeout = 30 * time.Second
const switchBuildModeTimeout = 5 * time.Second

// NewRiver creates the River from config
func NewRiver(c *Config, log loggers.Contextual, rebuildAndExit bool) (*River, error) {
	var err error
	r := new(River)

	r.Log = log

	r.l = log.WithFields("service", r.String())

	r.c = c

	r.done = make(chan struct{})

	r.syncC = make(chan interface{}, 40960)

	r.FatalErrC = make(chan error, 64)

	r.rebuildAndExit = rebuildAndExit

	r.rebuildInProgress = set.NewSet()

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if r.balancer, err = NewBalancerClient(r.c.Balancer); err != nil {
		return nil, errors.Trace(err)
	}

	r.master = newMasterState(r.c)

	r.StatService = &stat{r: r, RebuildLog: make([]buildLogRecord, 0)}

	r.sup = suture.New("river", suture.Spec{
		Timeout: 3 * time.Second,
		Log: func(msg string) {
			r.Log.WithFields("library", "suture").Info(msg)
		},
	})

	r.sphinxService = NewSphinxService(r)
	r.syncService = NewSyncService(r)

	if err = r.CheckBinlogRowImage(); err != nil {
		return nil, errors.Trace(err)
	}

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	cfg.Flavor = r.c.Flavor
	cfg.HeartbeatPeriod = r.c.HeartbeatPeriod.Duration

	cfg.IncludeTableRegex = []string{}
	for _, rule := range r.c.IngestRules {
		cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, regexp.QuoteMeta(rule.TableName))
	}

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = r.c.SkipMasterData

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

// CheckBinlogRowImage row image must be FULL
func (r *River) CheckBinlogRowImage() error {
	res, err := r.canal.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`)
	if err != nil {
		return errors.Trace(err)
	}
	rowImage, _ := res.GetString(0, 1)
	if !strings.EqualFold(rowImage, "FULL") {
		return errors.Errorf("MySQL uses '%s' binlog row image, but we want FULL", rowImage)
	}

	return nil
}

// Serve starts the River service
func (r *River) Serve() {
	r.m.Lock()
	r.isRunning = true
	r.m.Unlock()
	err := r.run()
	if err != nil {
		r.FatalErrC <- err
	}
	<-r.done
}

func (r *River) run() error {
	var err error

	r.ctx, r.Cancel = context.WithCancel(context.Background())

	r.sup.ServeBackground()

	r.sphinxService.RequestStartNotification()
	r.sphinxToken = r.sup.Add(r.sphinxService)
	r.sphinxService.WaitUntilStarted()

	err = r.initMasterState()
	if err != nil {
		return errors.Trace(err)
	}

	if r.rebuildAndExit {
		err = r.rebuildAll(nil, "rebuild-and-exit option is used")
	} else if r.master.needPositionReset {
		err = r.rebuildAll(nil, "reset flag was set")
	} else if err = r.sphinxService.LoadSyncState(r.master.syncState()); err != nil {
		err = r.rebuildAll(nil, fmt.Sprintf("one or more sphinx backends are not up to date: %v", err))
	} else {
		err = r.rebuildIfNotReady(nil)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if r.rebuildAndExit {
		return errors.Trace(ErrRebuildAndExitFlagSet)
	}

	r.master.needPositionReset = false

	if r.cronToken == nil {
		t := r.sup.Add(NewCronService(r))
		r.cronToken = &t
	}

	r.startSyncRoutine()

	return nil
}

// RebuildInProgress list of indexes that are being rebuilt right now
func (r *River) RebuildInProgress() []string {
	p := r.rebuildInProgress.ToSlice()
	indexList := make([]string, len(p))
	for i, index := range p {
		indexList[i] = index.(string)
	}
	return indexList
}

// Stop stops the River service
func (r *River) Stop() {
	r.m.Lock()
	defer r.m.Unlock()
	if !r.isRunning {
		return
	}

	r.l.Infof("stopping river")

	r.stopSyncRoutine()

	if r.cronToken != nil {
		err := r.sup.RemoveAndWait(*r.cronToken, cronServiceStopTimeout)
		if err != nil {
			r.l.Errorf("CronService failed to stop after waiting for %s", cronServiceStopTimeout)
		}
		r.cronToken = nil
	}

	r.Cancel()

	err := r.sup.RemoveAndWait(r.sphinxToken, sphinxServiceStopTimeout)
	if err != nil {
		r.l.Errorf("SphinxService failed to stop after waiting for %s", sphinxServiceStopTimeout)
	}

	r.sup.Stop()

	r.done <- struct{}{}
	r.isRunning = false
}

func (r *River) String() string {
	return "MainRiverService"
}

func (r *River) initMasterState() (err error) {
	m := r.master
	err = m.load()
	if err != nil {
		return errors.Trace(err)
	}
	r.l.Infof("master state: %s", m.String())
	if m.needPositionReset || (m.useGTID && m.gtid == nil) || m.pos == nil {
		r.l.Infof("resetting master state to the current upstream position")
		err = m.resetToCurrent(r.canal)
	}
	return
}

// SaveState saves current state to file and to sphinx backends
func (r *River) SaveState() {
	err := r.sphinxService.SaveSyncState()
	if err != nil {
		r.l.Errorf("could not save synchronization state: %s", errors.ErrorStack(err))
	}
}

func (r *River) startSyncRoutine() {
	r.syncM.Lock()
	defer r.syncM.Unlock()
	if r.syncToken == nil {
		t := r.sup.Add(r.syncService)
		r.syncToken = &t
	}
	if r.canalToken == nil {
		t := r.sup.Add(NewCanalService(r))
		r.canalToken = &t
	}
}

func (r *River) stopSyncRoutine() {
	r.syncM.Lock()
	defer r.syncM.Unlock()
	if r.canalToken != nil {
		err := r.sup.RemoveAndWait(*r.canalToken, canalServiceStopTimeout)
		if err != nil {
			r.l.Errorf("CanalService failed to stop after waiting for %s", canalServiceStopTimeout)
		}
		r.canalToken = nil
	}
	if r.syncToken != nil {
		err := r.sup.RemoveAndWait(*r.syncToken, syncServiceStopTimeout)
		if err != nil {
			r.l.Errorf("SyncService failed to stop after waiting for %s", syncServiceStopTimeout)
		}
		r.syncToken = nil
	}
}

func (r *River) enableBuildMode() error {
	r.syncM.Lock()
	defer r.syncM.Unlock()
	if r.syncToken == nil {
		r.l.Infof("did not enable build mode since river sync thread is not running")
		return nil
	}
	return errors.Trace(r.syncService.SwitchBuildMode(true, switchBuildModeTimeout))
}

func (r *River) disableBuildMode() error {
	r.syncM.Lock()
	defer r.syncM.Unlock()
	if r.syncToken == nil {
		r.l.Infof("did not disable build mode since river sync thread is not running")
		return nil
	}
	return errors.Trace(r.syncService.SwitchBuildMode(false, switchBuildModeTimeout))
}

func (r *River) startRebuildingIndexGroup(ctx context.Context, build indexGroupBuild) error {
	var cancelFunc context.CancelFunc
	if ctx == nil {
		ctx, cancelFunc = context.WithCancel(r.ctx)
	}
	build.logger.Info("rebuild start")
	r.StatService.logRebuildStart(build)
	err := rebuildIndexGroup(ctx, r, build)
	if err != nil {
		build.logger.Errorf("rebuild failed: %s", errors.ErrorStack(err))
	} else {
		build.logger.Info("rebuild done")
	}
	r.StatService.logRebuildFinish(build.id, err)
	if cancelFunc != nil {
		cancelFunc()
	}

	return err
}

func (r *River) checkAllIndexesForOptimize() {
	for index, indexConfig := range r.c.DataSource {
		err := r.sphinxService.CheckIndexForOptimize(index, indexConfig.Parts)
		if err != nil {
			log.Warnf("periodic optimize error: %s", errors.ErrorStack(err))
		}
	}
}

// rebuildAll rebuilds all configured indexes
func (r *River) rebuildAll(ctx context.Context, reason string) error {
	return r.rebuildIfNot(
		ctx,
		reason,
		func(string, *SourceConfig) (bool, error) {
			return false, nil
		},
	)
}

func (r *River) rebuildIfNotReady(ctx context.Context) error {
	isReady := func(index string, cfg *SourceConfig) (bool, error) {
		return r.sphinxService.IndexIsReady(index, cfg.Parts)
	}
	return r.rebuildIfNot(ctx, "index is not ready", isReady)
}

func (r *River) rebuildIfNot(
	ctx context.Context,
	reason string,
	predicate func(string, *SourceConfig) (bool, error),
) (err error) {
	indexes := []string{}
	for index, cfg := range r.c.DataSource {
		skipRebuild, err := predicate(index, cfg)
		if err != nil {
			return errors.Trace(err)
		}

		if !skipRebuild {
			indexes = append(indexes, index)
		}
	}

	if len(indexes) == 0 {
		return nil
	}

	var rebuildState *RebuildStartState
	if len(indexes) == len(r.c.DataSource) {
		rebuildState, err = NewRebuildStartState(r.canal)
		if err != nil {
			return errors.Trace(err)
		}
		r.l.Infof("rebuild indexes from GTID: %s", rebuildState.gtid)
	}

	r.l.Infof("will rebuild indexes %s: %s", strings.Join(indexes, ","), reason)

	build, err := newIndexGroupBuild(r.c, r.Log, indexes, uuid.NewV1, rebuildState)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.startRebuildingIndexGroup(ctx, *build)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func rowCount(e *canal.RowsEvent) int {
	rows := len(e.Rows)
	if e.Action == canal.UpdateAction {
		rows = rows / 2
	}
	return rows
}

func executeMysqlQuery(canal *canal.Canal, query string) error {
	log.Infof("[mysql] %s", query)
	_, err := canal.Execute(query)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
