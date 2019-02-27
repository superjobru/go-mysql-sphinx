package river

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

const rawReply = `1
# be_id be_name srv_id srv_name srv_addr srv_op_state srv_admin_state srv_uweight srv_iweight srv_time_since_last_change srv_check_status srv_check_result srv_check_health srv_check_state srv_agent_state bk_f_forced_id srv_f_forced_id srv_fqdn srv_port
3 my_resume_cluster 8 searchd.local 172.27.0.1 2 0 56 56 1212075 15 3 4 6 0 0 0 searchd.local 5001
5 my_vacancy_cluster 8 searchd.local 172.27.0.1 0 5 56 56 1212075 1 0 0 14 0 0 0 searchd.local 5002

`

var decodedReply = []*ServerStateInfo{
	&ServerStateInfo{
		BackendID:               3,
		BackendName:             "my_resume_cluster",
		ID:                      8,
		Name:                    "searchd.local",
		Addr:                    "172.27.0.1",
		OperationalState:        2,
		AdminState:              0,
		ActualWeight:            56,
		InitialWeight:           56,
		TimeSinceLastChange:     1212075,
		LastHealthCheckStatus:   15,
		LastHealthCheckResult:   3,
		HealthStatusChangeCount: 4,
		HealthCheckState:        6,
		AgentCheckState:         0,
		BackendIDIsSetByConfig:  false,
		IDIsSetByConfig:         false,
		FQDN:                    "searchd.local",
		Port:                    5001,
	},
	&ServerStateInfo{
		BackendID:               5,
		BackendName:             "my_vacancy_cluster",
		ID:                      8,
		Name:                    "searchd.local",
		Addr:                    "172.27.0.1",
		OperationalState:        0,
		AdminState:              5,
		ActualWeight:            56,
		InitialWeight:           56,
		TimeSinceLastChange:     1212075,
		LastHealthCheckStatus:   1,
		LastHealthCheckResult:   0,
		HealthStatusChangeCount: 0,
		HealthCheckState:        14,
		AgentCheckState:         0,
		BackendIDIsSetByConfig:  false,
		IDIsSetByConfig:         false,
		FQDN:                    "searchd.local",
		Port:                    5002,
	},
}

func Test_decodeShowServerStateReply(t *testing.T) {
	tests := []struct {
		name    string
		reply   string
		want    []*ServerStateInfo
		wantErr bool
	}{
		{
			name:    "valid reply",
			reply:   rawReply,
			want:    decodedReply,
			wantErr: false,
		},
		{
			name:    "unexpected reply",
			reply:   "The stars are not aligned.",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeShowServerStateReply(tt.reply)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeShowServerStateReply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeShowServerStateReply() = %v, want %v", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}

func Test_findServerInfo(t *testing.T) {
	type args struct {
		rowset         []*ServerStateInfo
		backend        string
		sphinxHostname string
	}
	tests := []struct {
		name    string
		args    args
		want    *ServerStateInfo
		wantErr bool
	}{
		{
			name: "test valid lookup by FQDN",
			args: args{
				rowset:         decodedReply,
				backend:        "my_vacancy_cluster",
				sphinxHostname: "searchd.local",
			},
			want:    decodedReply[1],
			wantErr: false,
		},
		{
			name: "test valid lookup by IP address",
			args: args{
				rowset:         decodedReply,
				backend:        "my_vacancy_cluster",
				sphinxHostname: "172.27.0.1",
			},
			want:    decodedReply[1],
			wantErr: false,
		},
		{
			name: "test existing backend but missing connection",
			args: args{
				rowset:         decodedReply,
				backend:        "my_vacancy_cluster",
				sphinxHostname: "uganda",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test missing backend",
			args: args{
				rowset:         decodedReply,
				backend:        "sphinx_whatever_cluster",
				sphinxHostname: "searchd.local",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findServerInfo(tt.args.rowset, tt.args.backend, tt.args.sphinxHostname)
			if (err != nil) != tt.wantErr {
				t.Errorf("findServerInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findServerInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}
