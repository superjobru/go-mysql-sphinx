package river

import (
	"bytes"
	"encoding/csv"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/juju/errors"
)

// ServerStateInfo this is returned by "show servers state" command
type ServerStateInfo struct {
	BackendID               int    `csv:"be_id"`
	BackendName             string `csv:"be_name"`
	ID                      int    `csv:"srv_id"`
	Name                    string `csv:"srv_name"`
	Addr                    string `csv:"srv_addr"`
	OperationalState        uint8  `csv:"srv_op_state"`
	AdminState              uint8  `csv:"srv_admin_state"`
	ActualWeight            uint8  `csv:"srv_uweight"`
	InitialWeight           uint8  `csv:"srv_iweight"`
	TimeSinceLastChange     uint64 `csv:"srv_time_since_last_change"`
	LastHealthCheckStatus   int16  `csv:"srv_check_status"`
	LastHealthCheckResult   uint8  `csv:"srv_check_result"`
	HealthStatusChangeCount uint8  `csv:"srv_check_health"`
	HealthCheckState        uint8  `csv:"srv_check_state"`
	AgentCheckState         uint8  `csv:"srv_agent_state"`
	BackendIDIsSetByConfig  bool   `csv:"bk_f_forced_id"`
	IDIsSetByConfig         bool   `csv:"srv_f_forced_id"`
	FQDN                    string `csv:"srv_fqdn"`
	Port                    uint16 `csv:"srv_port"`
}

const (
	adminStateForcedMaint = 1 << iota
	adminStateInheritedMaint
	adminStateConfigMaint
	adminStateForcedDrain
	adminStateInheritedDrain
	adminStateIPResolutionMaint
	adminStateForcedFQDN
)

var errMissingServerState = errors.New("requested server state not found")

func decodeShowServerStateReply(reply string) ([]*ServerStateInfo, error) {
	if !strings.HasPrefix(reply, expectedShowStateReplyPrefix) {
		return nil, errors.Annotatef(errBalancerQueryError, "error requesting server states: %s", reply)
	}
	payload := strings.TrimPrefix(reply, expectedShowStateReplyPrefix)
	buf := bytes.NewBufferString(payload)
	reader := csv.NewReader(buf)
	reader.Comma = ' '
	reader.Comment = '#'
	info := []*ServerStateInfo{}
	if err := gocsv.UnmarshalCSV(reader, &info); err != nil {
		return nil, errors.Annotatef(err, "error decoding server states: %s", reply)
	}
	return info, nil
}

func findServerInfo(rowset []*ServerStateInfo, backend, sphinxHostname string) (*ServerStateInfo, error) {
	if rowset != nil {
		for _, row := range rowset {
			if row == nil {
				continue
			}
			if row.BackendName != backend {
				continue
			}
			if row.FQDN == sphinxHostname || row.Addr == sphinxHostname {
				return row, nil
			}
		}
	}
	return nil, errors.Annotatef(
		errMissingServerState,
		"backend '%s'; sphinx connection '%s'",
		backend,
		sphinxHostname,
	)
}

func (i *ServerStateInfo) isManagementAllowed() (result bool, reason string) {
	if i.AdminState&adminStateConfigMaint != 0 {
		return false, "server is set to maintenance by configuration"
	} else if i.AdminState&adminStateForcedMaint != 0 {
		return false, "server is set to maintenance manually"
	}
	return true, ""
}
