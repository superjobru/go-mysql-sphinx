package river

import (
	"context"
	"sync"

	"github.com/robfig/cron"
	"github.com/superjobru/go-mysql-sphinx/util"
	"gopkg.in/birkirb/loggers.v1"
)

// CronService represents a service that does periodic jobs
type CronService struct {
	riverInstance *River
	cron          *cron.Cron
	cancel        context.CancelFunc
	ctx           context.Context
	log           loggers.Advanced
	wg            sync.WaitGroup
}

// Serve suture.Service implementation
func (s *CronService) Serve() {
	s.log.Info("Serve() started")
	s.wg.Add(1)
	defer func() {
		s.wg.Done()
		s.log.Info("Serve() exited")
	}()
	stdLogger, logWriter := util.NewStdLogger(s.riverInstance.Log)
	defer logWriter.Close()
	s.cron = cron.New()
	s.cron.ErrorLog = stdLogger
	cfg := s.riverInstance.c.MaintenanceConfig
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if cfg.OptimizeSchedule != "" {
		s.cron.AddFunc(cfg.OptimizeSchedule, func() {
			s.log.Info("starting optimize job")
			s.riverInstance.checkAllIndexesForOptimize()
		})
	}
	if cfg.RebuildSchedule != "" {
		s.cron.AddFunc(cfg.RebuildSchedule, func() {
			s.log.Info("starting rebuild job")
			s.riverInstance.rebuildAll(s.ctx, "scheduled rebuild")
		})
	}
	s.cron.Run()
}

// Stop suture.Service implementation
func (s *CronService) Stop() {
	if s.cron != nil {
		s.cron.Stop()
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *CronService) String() string {
	return "CronService"
}

// NewCronService service constructor
func NewCronService(r *River) *CronService {
	s := &CronService{
		riverInstance: r,
	}
	s.log = s.riverInstance.Log.WithFields("service", "CronService")
	return s
}
