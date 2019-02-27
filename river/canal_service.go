package river

import (
	"context"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/birkirb/loggers.v1"
)

// CanalService the one that really reads binlog
type CanalService struct {
	log           loggers.Advanced
	riverInstance *River
}

// Serve suture.Service implementation
func (s *CanalService) Serve() {
	var err error

	r := s.riverInstance

	err = r.newCanal()
	if err != nil {
		s.log.Errorf("could not create canal instance: %s", errors.ErrorStack(err))
		return
	}

	r.canal.SetEventHandler(newReplicationEventHandler(r))

	if r.c.ReplayFullBinlog {
		s.log.Infof("replay_full_binlog is set, will start without setting replication state")
		err = r.canal.Run()
	} else {
		if r.c.UseGTID {
			if r.master.gtidString() == "" {
				s.log.Infof("use_gtid is set, bit GTID is empty, will start without setting replication state")
				err = r.canal.Run()
			} else {
				gtid := r.master.gtidSet()
				s.log.Infof("use_gtid is set, will start from '%s' GTID", gtid)
				err = r.canal.StartFromGTID(gtid)
			}
		} else {
			pos := r.master.position()
			s.log.Infof("use_gtid is not set, will start from '%s' position", pos)
			err = r.canal.RunFrom(pos)
		}
	}

	if err != nil {
		if isReplicationError(err) {
			r.master.needPositionReset = true
			s.log.Infof("replication error detected: will reset position and rebuild")
			r.Stop()
		}
		if errors.Cause(err) == context.Canceled {
			s.log.Info("canal stopped")
		} else {
			s.log.Errorf("canal stopped with error: %s", errors.ErrorStack(err))
		}
	}
}

// Stop suture.Service implementation
func (s *CanalService) Stop() {
	s.riverInstance.canal.Close()
}

func (s CanalService) String() string {
	return "CanalService"
}

// NewCanalService service constructor
func NewCanalService(r *River) *CanalService {
	s := CanalService{riverInstance: r}
	s.log = r.Log.WithFields("service", s.String())
	return &s
}

func isReplicationError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MyError)
	if !ok {
		return false
	}
	return mysqlErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG
}
