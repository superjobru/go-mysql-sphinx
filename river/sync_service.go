package river

import (
	"context"
	"sync"
	"time"

	"github.com/juju/errors"
	"gopkg.in/birkirb/loggers.v1"
)

type buildModeMsg struct {
	enabled bool
	replyTo chan struct{}
}

// SyncService represents a service that does periodic jobs
type SyncService struct {
	ctx           context.Context
	cancel        context.CancelFunc
	log           loggers.Advanced
	riverInstance *River
	switchC       chan buildModeMsg
	wg            sync.WaitGroup
}

var errSwitchBuildModeTimeout = errors.New("timeout exceeded while trying to switch build mode")

// Serve suture.Service implementation
func (s *SyncService) Serve() {
	s.log.Info("Serve() started")
	s.wg.Add(1)
	defer func() {
		s.wg.Done()
		s.log.Info("Serve() exited")
	}()
	s.ctx, s.cancel = context.WithCancel(s.riverInstance.ctx)
	s.SyncLoop(s.ctx)
}

// Stop suture.Service implementation
func (s *SyncService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *SyncService) String() string {
	return "SyncService"
}

// SwitchBuildMode turns on or off so-called "build mode"
// The worker in this mode processes events as usual,
// but does not save position to be resumed from.
// This is useful when doing maintenance rebuilds, hence "build mode".
func (s *SyncService) SwitchBuildMode(isEnabled bool, timeout time.Duration) error {
	var timeoutC <-chan time.Time

	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutC = timer.C
	}

	replyC := make(chan struct{})
	defer close(replyC)

	select {
	case <-timeoutC:
		return errSwitchBuildModeTimeout
	case s.switchC <- buildModeMsg{enabled: isEnabled, replyTo: replyC}:
	}
	select {
	case <-timeoutC:
		return errSwitchBuildModeTimeout
	case <-replyC:
	}
	return nil
}

// NewSyncService constructor
func NewSyncService(r *River) *SyncService {
	s := SyncService{riverInstance: r, switchC: make(chan buildModeMsg, 16)}
	s.log = r.Log.WithFields("service", s.String())
	return &s
}
