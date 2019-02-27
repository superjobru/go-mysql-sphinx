package river

import (
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go/sync2"
	"github.com/superjobru/go-mysql-sphinx/sphinx"
	"gopkg.in/birkirb/loggers.v1"
	"gopkg.in/birkirb/loggers.v1/log"
)

// SphinxService represents a service that does communication with Sphinx
type SphinxService struct {
	log           loggers.Advanced
	riverInstance *River
	// sphinx connections
	sph []*sphinx.SphConn
	// protects `sph` slice
	sphm    sync.Mutex
	started chan struct{}
	stop    chan struct{}
}

// Serve suture.Service implementation
func (s *SphinxService) Serve() {
	s.log.Info("Starting")
	s.connect()
	defer s.disconnect()
	if s.started != nil {
		s.started <- struct{}{}
	}
	<-s.stop
	s.log.Infof("Serve() exited")
}

// Stop suture.Service implementation
func (s *SphinxService) Stop() {
	s.stop <- struct{}{}
}

// RequestStartNotification must be called prior to WaitUntilStarted
func (s *SphinxService) RequestStartNotification() {
	s.started = make(chan struct{})
}

// WaitUntilStarted waits until service is started
func (s *SphinxService) WaitUntilStarted() {
	<-s.started
	s.started = nil
}

func (s *SphinxService) String() string {
	return "SphinxService"
}

// NewSphinxService constructor
func NewSphinxService(r *River) *SphinxService {
	s := SphinxService{riverInstance: r, stop: make(chan struct{})}
	s.log = r.Log.WithFields("service", s.String())
	return &s
}

func (s *SphinxService) connect() {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	var err error
	r := s.riverInstance
	connSettings := sphinx.ConnSettings{
		DisconnectRetryDelay: r.c.SphConnSettings.DisconnectRetryDelay.Duration,
		OverloadRetryDelay:   r.c.SphConnSettings.OverloadRetryDelay.Duration,
	}
	s.sph, err = sphinx.ConnectMany(r.c.SphAddr, connSettings, nil)
	if err != nil {
		r.FatalErrC <- errors.Annotatef(err, "could not connect to sphinx")
		return
	}
}

func (s *SphinxService) disconnect() {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	sphinx.CloseMany(s.sph)
}

// LoadSyncState ...
func (s *SphinxService) LoadSyncState(defaultState sphinx.SyncState) error {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	if s.sph == nil {
		return errors.Annotatef(errSphinxDisconnected, "error loading synchronization state")
	}
	if s.riverInstance.c.SkipSphSyncState {
		log.Info("use skip_sph_sync_state option, skipped loading synchronization state from sphinx")
		return nil
	}
	if len(s.sph) == 0 {
		log.Info("no sphinx backends configured, skipped loading synchronization state from sphinx")
		return nil
	}
	state, err := sphinx.LoadSyncState(s.sph, defaultState)
	if err != nil {
		return errors.Trace(err)
	}
	if state == nil {
		log.Info("no synchronization state saved on sphinx backends, using already loaded state")
		return nil
	}
	s.riverInstance.master.updatePosition(positionEvent{
		gtid: state.GTID,
		pos:  state.Position,
	})
	log.Infof("updated master position to %s", state.GTID)
	return nil
}

// SaveSyncState ...
func (s *SphinxService) SaveSyncState() (err error) {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	var master = s.riverInstance.master
	if err = master.save(); err != nil {
		return errors.Trace(err)
	}
	if s.sph == nil {
		return errors.Trace(errSphinxDisconnected)
	}
	return errors.Trace(sphinx.SaveSyncState(s.sph, master.syncState()))
}

// IndexIsReady ...
func (s *SphinxService) IndexIsReady(index string, parts uint16) (bool, error) {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	if s.sph == nil {
		return false, errors.Annotatef(errSphinxDisconnected, "error checking that index %s is ready", index)
	}
	return sphinx.IndexIsReady(s.sph, index, parts)
}

// ReloadRtIndex ...
func (s *SphinxService) ReloadRtIndex(build indexGroupBuild) error {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	if s.sph == nil {
		return errors.Annotatef(errSphinxDisconnected, "error reloading after the build %s", build.id)
	}
	return errors.Trace(s.riverInstance.balancer.RollingReloadIndex(s.sph, build))
}

// CheckIndexForOptimize ...
func (s *SphinxService) CheckIndexForOptimize(index string, parts uint16) error {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	errFormat := "error trying to optimize index %s"
	if s.sph == nil {
		return errors.Annotatef(errSphinxDisconnected, errFormat, index)
	}
	// TODO after the latest overhaul this check shouldn't be neccessary anymore
	if s.riverInstance.rebuildInProgress.Contains(index) {
		return errors.Annotatef(errIndexRebuildInProgress, errFormat, index)
	}
	config := s.riverInstance.c
	return errors.Annotatef(
		sphinx.CheckIndexForOptimize(
			s.sph,
			index,
			parts,
			sphinx.IndexMaintenanceSettings{
				MaxRAMChunkBytes: config.MaintenanceConfig.MaxRAMChunkBytes,
			},
		),
		errFormat,
		index,
	)
}

// Query ...
func (s *SphinxService) Query(stmt string, counter *sync2.AtomicUint64) error {
	s.sphm.Lock()
	defer s.sphm.Unlock()
	if s.sph == nil {
		return errors.Trace(errSphinxDisconnected)
	}
	resultList := sphinx.ExecuteInParallel(s.sph, stmt)
	for i, res := range resultList {
		if res.Err != nil {
			return errors.Annotatef(res.Err, "error executing query on server#%d (%s)", i, s.sph[i].RemoteAddr())
		}
		counter.Add(res.Result.AffectedRows)
	}
	return nil
}
