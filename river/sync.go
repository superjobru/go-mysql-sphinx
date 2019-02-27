package river

import (
	"context"
	"fmt"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type positionEvent struct {
	gtid  mysql.GTIDSet
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r             *River
	activeGTID    mysql.GTIDSet
	completedGTID mysql.GTIDSet
	pos           mysql.Position
}

type gtidSubscriber struct {
	c    chan mysql.GTIDSet
	gtid mysql.GTIDSet
	peer string
}

type gtidCancelSubscriber struct {
	peer   string
	reason string
}

type syncState struct {
	ctx             context.Context
	queue           BatchedChangeSet
	staged          BatchedChangeSet
	gtidSubscribers map[string]map[mysql.GTIDSet][]gtidSubscriber
	// to prevent memory hogging, one should only remember events that were not synced yet
	firstPositionEventID uint64
	// this gets incremented on every position event; used to fill TableRowChange.EventID
	lastPositionEventID uint64
	// position events that were not synced yet (except the first one)
	// the head of this slice moves forward with every batch processed
	positionEvents   []positionEvent
	flushC           <-chan time.Time
	flushTimer       *time.Timer
	buildModeC       <-chan buildModeMsg
	buildModeEnabled bool
}

type syncActionNeed struct {
	flush bool
	stop  bool
}

func (p positionEvent) String() string {
	return fmt.Sprintf("GTID: %s; Position: %s", p.gtid, p.pos)
}

func newReplicationEventHandler(r *River) *eventHandler {
	return &eventHandler{
		r:             r,
		activeGTID:    r.master.gtidSet(),
		completedGTID: r.master.gtidSet(),
		pos:           r.master.position(),
	}
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	h.pos = mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.sendPositionEvent(true)

	return h.r.ctx.Err()
}

func (h *eventHandler) OnTableChanged(schema, table string) error {
	// TODO: update rules accordingly?
	return nil
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, e *replication.QueryEvent) error {
	if err := h.updateGTID(e.GSet); err != nil {
		return err
	}
	h.pos = nextPos
	h.sendPositionEvent(true)
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.pos = nextPos
	h.completedGTID = h.activeGTID.Clone()
	forceFlush := h.r.c.FlushBulkTime.Duration == 0
	h.sendPositionEvent(forceFlush)
	return h.r.ctx.Err()
}

func (h *eventHandler) OnGTID(gtid mysql.GTIDSet) error {
	if err := h.updateGTID(gtid); err != nil {
		return err
	}
	h.sendPositionEvent(false)
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	docCount, err := ApplyRuleSet(h.r.c.IngestRules, e, h.r.syncC)
	stat := h.r.StatService
	stat.lastRowEvent = time.Unix(int64(e.Header.Timestamp), 0)
	stat.ProcessedRowEvents.Add(1)
	stat.ProcessedDocEvents.Add(docCount)
	return err
}

func (h *eventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	h.pos = pos
	h.sendPositionEvent(force)
	return h.r.ctx.Err()
}

func (h *eventHandler) String() string {
	return "SphRiverEventHandler"
}

func (h *eventHandler) sendPositionEvent(forceFlush bool) {
	var gtid mysql.GTIDSet
	if h.completedGTID != nil {
		gtid = h.completedGTID.Clone()
	}
	h.r.syncC <- positionEvent{gtid, h.pos, forceFlush}
}

func (h *eventHandler) updateGTID(gtid mysql.GTIDSet) error {
	if gtid != nil {
		switch v := gtid.(type) {
		case *mysql.MariadbGTIDSet:
			for _, subset := range v.Sets {
				if err := h.activeGTID.(*mysql.MariadbGTIDSet).AddSet(subset); err != nil {
					return err
				}
			}
		default:
			h.activeGTID = gtid
		}
	}
	return nil
}

// SyncLoop worker that groups database updates and applies them to Sphinx.
// Intended to run in a separate goroutine
func (s *SyncService) SyncLoop(ctx context.Context) {
	var flushTicker *time.Ticker
	r := s.riverInstance

	state := syncState{
		ctx:             ctx,
		queue:           NewBatchedChangeSet("queue"),
		staged:          NewBatchedChangeSet("staged"),
		gtidSubscribers: map[string]map[mysql.GTIDSet][]gtidSubscriber{},
		positionEvents: []positionEvent{
			// Make sure that this slice is non-empty to prevent stupid off-by-one errors.
			positionEvent{
				gtid: r.master.gtidSet(),
				pos:  r.master.position(),
			},
		},
		buildModeC: s.switchC,
	}

	defer r.SaveState()

	flushInterval := r.c.FlushBulkTime.Duration
	if flushInterval > 0 {
		flushTicker = time.NewTicker(flushInterval)
		state.flushC = flushTicker.C
		defer flushTicker.Stop()
	}

	for {
		need := state.acceptMessage(s)

		if need.stop {
			break
		}

		if need.flush {
			now := time.Now()
			state.SplitQueue(func(c DocumentChange) bool {
				return now.Sub(c.LastTS) >= r.c.MinTimeAfterLastEvent.Duration ||
					now.Sub(c.FirstTS) >= r.c.MaxTimeAfterFirstEvent.Duration
			})
			if err := r.doBatch(state.staged); err != nil {
				s.log.Errorf("do sphinx bulk err %v, close sync", err)
				r.FatalErrC <- err
				break
			}

			if r.c.FlushBulkTime.Duration == 0 && len(state.queue.Changes) > 0 {
				if state.flushTimer != nil {
					state.flushTimer.Stop()
				}
				if r.c.MinTimeAfterLastEvent.Duration > 0 {
					state.flushTimer = time.NewTimer(r.c.MinTimeAfterLastEvent.Duration)
					state.flushC = state.flushTimer.C
				}
			}

			syncedPositionEventID := state.queue.FirstSeenPositionEventID(state.lastPositionEventID)
			offset := syncedPositionEventID - state.firstPositionEventID
			state.firstPositionEventID = syncedPositionEventID
			state.positionEvents = state.positionEvents[offset:]

			if !state.buildModeEnabled {
				if r.master.updatePosition(state.positionEvents[0]) {
					r.SaveState()
					sc := state.processGTIDSubscribers()
					if sc > 0 {
						s.log.Infof("notified %d gtid subscribers", sc)
					}
				}
			}
		}
	}
}

func (s *syncState) acceptMessage(srv *SyncService) syncActionNeed {
	need := syncActionNeed{
		flush: false,
		stop:  false,
	}

	select {
	case msg := <-srv.switchC:
		s.buildModeEnabled = msg.enabled
		srv.log.Infof("switched build mode to %v at %s", s.buildModeEnabled, s.positionEvents[0])
		msg.replyTo <- struct{}{}
	case v := <-srv.riverInstance.syncC:
		switch v := v.(type) {
		case positionEvent:
			s.positionEvents = append(s.positionEvents, v)
			s.lastPositionEventID++
			if v.force {
				need.flush = true
			}
		case TableRowChange:
			v.EventID = s.lastPositionEventID
			s.queue.AddRowChange(v)
		case gtidSubscriber:
			s.processGTIDSubscribeRequest(srv, v)
		case gtidCancelSubscriber:
			delete(s.gtidSubscribers, v.peer)
			srv.log.Infof("deleted subscriptions for gtid from peer %s: %s", v.peer, v.reason)
		}
	case <-s.flushC:
		if srv.riverInstance.c.FlushBulkTime.Duration == 0 {
			s.flushC = nil
			s.flushTimer = nil
		}
		need.flush = true
	case <-s.ctx.Done():
		need.stop = true
	}
	return need
}

func (s *syncState) processGTIDSubscribeRequest(srv *SyncService, gs gtidSubscriber) {
	if gs.gtid == nil {
		return
	}
	currentGTID := srv.riverInstance.master.gtidSet()
	gtid := gs.gtid
	peer := gs.peer
	if currentGTID.Contain(gtid) {
		gs.c <- currentGTID
	} else {
		if _, ok := s.gtidSubscribers[peer]; !ok {
			s.gtidSubscribers[peer] = map[mysql.GTIDSet][]gtidSubscriber{}
		}
		if _, ok := s.gtidSubscribers[peer][gtid]; !ok {
			s.gtidSubscribers[peer][gtid] = []gtidSubscriber{}
		}
		s.gtidSubscribers[peer][gtid] = append(s.gtidSubscribers[peer][gtid], gs)
		srv.log.Infof("added subscription for gtid %s from peer %s", gtid, peer)
	}
}

func (s *syncState) processGTIDSubscribers() (notifiedSubscribersCount int) {
	newGTID := s.positionEvents[0].gtid
	if newGTID == nil {
		return
	}
	for _, peerSubs := range s.gtidSubscribers {
		done := []mysql.GTIDSet{}
		for gtid, subs := range peerSubs {
			if newGTID.Contain(gtid) {
				for _, sub := range subs {
					sub.c <- newGTID
				}
				notifiedSubscribersCount += len(subs)
				done = append(done, gtid)
			}
		}
		for _, doneGTID := range done {
			delete(peerSubs, doneGTID)
		}
	}
	return
}

// SplitQueue splits current queue of document changes
// changes that are ready to be processed go to staged state
// other changes stay queued
// isReady argument is mainly for testing
func (s *syncState) SplitQueue(isReady func(c DocumentChange) bool) {
	ready, notReady := NewBatchedChangeSetPair()
	for _, docBatch := range s.queue.Changes {
		for _, doc := range docBatch.ChangedDocs {
			if isReady(doc) {
				ready.AddDocumentChange(doc)
			} else {
				notReady.AddDocumentChange(doc)
			}
		}
	}
	s.staged = ready
	s.queue = notReady
}
