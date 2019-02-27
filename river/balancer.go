package river

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/superjobru/go-mysql-sphinx/sphinx"
	"gopkg.in/birkirb/loggers.v1/log"
)

const (
	socketSchema = "unix://"
	tcpSchema    = "tcp://"
)

const (
	disabledState                = "maint"
	enabledState                 = "ready"
	expectedStateChangeReply     = "\n"
	expectedShowStateReplyPrefix = "1\n# "
)

var errBadAddrSchema = errors.New("unknown schema in the socket address")
var errBadCAFile = errors.New("error reading CA certificate")
var errBalancerQueryError = errors.New("error executing query to balancer")

// BalancerConn wrapper struct for a connection to a load balancer
// used for storing address and network, which may be useful in error descriptions
type BalancerConn struct {
	Conn    net.Conn
	Addr    string
	Network string
}

// BalancerClient allows enabling and disabling a server in a load balancer set
type BalancerClient struct {
	Config      BalancerConfig
	Connections []BalancerConn
	TLSConfig   *tls.Config
}

type setServerStateArgs struct {
	conn        []BalancerConn
	backendName string
	serverName  string
}

// NewBalancerClient constructor
func NewBalancerClient(cfg BalancerConfig) (*BalancerClient, error) {
	tlsConfig, err := balancerTLSConfig(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b := &BalancerClient{
		Config:      cfg,
		Connections: make([]BalancerConn, len(cfg.Addr)),
		TLSConfig:   tlsConfig,
	}
	for i, addr := range b.Config.Addr {
		if strings.HasPrefix(addr, tcpSchema) {
			b.Connections[i] = BalancerConn{
				Addr:    strings.Replace(addr, tcpSchema, "", 1),
				Network: "tcp",
			}
		} else if strings.HasPrefix(addr, socketSchema) {
			b.Connections[i] = BalancerConn{
				Addr:    strings.Replace(addr, socketSchema, "", 1),
				Network: "unix",
			}
		} else {
			return nil, errors.Annotatef(errBadAddrSchema, "bad address for balancer #%d (%s)", i, addr)
		}
	}
	return b, nil
}

// getServerState Retrieves current state of backend servers
// to prevent re-enabling servers that may be down for maintenance.
// Returns two-dimensional slice of ServerStateInfo structs,
// where first dimension is a balancer number in app configuration,
// and second dimension is row number as returned by HAProxy.
func (b *BalancerClient) getServerState() ([][]*ServerStateInfo, error) {
	replies, err := b.doQuery(b.Connections, showServersStateCommand())
	if err != nil {
		return nil, errors.Trace(err)
	}
	state := make([][]*ServerStateInfo, len(b.Config.Addr))
	for i, reply := range replies {
		info, err := decodeShowServerStateReply(reply)
		if err != nil {
			return nil, errors.Annotatef(err, "could not request state of balancer #%d (%s)", i, b.Config.Addr[i])
		}
		state[i] = info
	}
	return state, nil
}

// RollingReloadIndex reloading procedure that tries to disable server while reloading index there
// Does it sequentially - just one server at a time.
func (b *BalancerClient) RollingReloadIndex(sph []*sphinx.SphConn, build indexGroupBuild) error {
	var pause time.Duration
	state, err := b.getServerState()
	if err != nil {
		return errors.Trace(err)
	}
	for _, conn := range sph {
		argSet, err := b.gatherServerSetStateArgs(state, conn.Hostname())
		if err != nil {
			return errors.Trace(err)
		}
		b.setServerStateMultiple(argSet, disabledState)
		pause = b.Config.PauseAfterDisabling.Duration
		if pause > 0 && len(argSet) > 0 {
			log.Infof("[balancer] waiting for %s after disabling %s", pause, conn.Hostname())
			time.Sleep(pause)
		}
		for _, indexBuild := range build.indexes {
			err = conn.ReloadRtIndex(indexBuild.index, indexBuild.parts)
			if err != nil {
				return errors.Trace(err)
			}
		}
		pause = b.Config.PauseBeforeEnabling.Duration
		if pause > 0 && len(argSet) > 0 {
			log.Infof("[balancer] waiting for %s before enabling %s", pause, conn.Hostname())
			time.Sleep(pause)
		}
		b.setServerStateMultiple(argSet, enabledState)
	}
	return nil
}

func (b *BalancerClient) gatherServerSetStateArgs(state [][]*ServerStateInfo, sphinxHostname string) ([]setServerStateArgs, error) {
	a := []setServerStateArgs{}
	for _, backend := range b.Config.BackendName {
		for k, balancer := range b.Connections {
			srv, err := findServerInfo(state[k], backend, sphinxHostname)
			if err != nil {
				return nil, errors.Annotatef(err, "server state not found at balancer '%s'", balancer.Addr)
			}
			allowed, reason := srv.isManagementAllowed()
			if !allowed {
				log.Infof("[balancer] will not manage '%s' in '%s@%s': %s", sphinxHostname, backend, balancer.Addr, reason)
			} else {
				a = append(a, setServerStateArgs{
					conn: []BalancerConn{
						BalancerConn{
							Addr:    balancer.Addr,
							Network: balancer.Network,
						},
					},
					backendName: srv.BackendName,
					serverName:  srv.Name,
				})
			}
		}
	}
	return a, nil
}

func (b *BalancerClient) setServerState(connections []BalancerConn, backend, serverName, stateName string) error {
	var err error
	replies, err := b.doQuery(connections, setServerStateCommand(backend, serverName, stateName))
	if err != nil {
		return errors.Trace(err)
	}
	for i, reply := range replies {
		if reply != expectedStateChangeReply {
			return errors.Annotatef(errBalancerQueryError, "error setting state to '%s' for server '%s' in backend '%s@%s'", stateName, serverName, backend, connections[i].Addr)
		}
	}
	return err
}

func (b *BalancerClient) setServerStateMultiple(argSet []setServerStateArgs, stateName string) {
	for _, args := range argSet {
		err := b.setServerState(args.conn, args.backendName, args.serverName, stateName)
		if err != nil {
			log.Warnf("[balancer] %s", errors.ErrorStack(err))
		}
	}
}

func balancerTLSConfig(cfg BalancerConfig) (*tls.Config, error) {
	if cfg.UseTLS {
		var caCertPool *x509.CertPool
		var err error
		if cfg.ServerCAFile == "" {
			caCertPool, err = x509.SystemCertPool()
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			caCertPool = x509.NewCertPool()
			caCert, err := ioutil.ReadFile(cfg.ServerCAFile)
			if err != nil {
				return nil, errors.Annotatef(err, "could not read server CA file %s", cfg.ServerCAFile)
			}
			ok := caCertPool.AppendCertsFromPEM(caCert)
			if !ok {
				return nil, errors.Annotatef(errBadCAFile, "error creating CA cert pool")
			}
		}
		clientCert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
			ServerName:   cfg.TLSServerName,
		}, nil
	}
	return nil, nil
}

func (b *BalancerClient) connect(connections []BalancerConn) ([]BalancerConn, error) {
	var err error
	connSet := make([]BalancerConn, 0, len(connections))
	d := &net.Dialer{Timeout: b.Config.Timeout.Duration}
	for _, c := range connections {
		var conn net.Conn
		if b.Config.UseTLS {
			conn, err = tls.DialWithDialer(d, c.Network, c.Addr, b.TLSConfig)
		} else {
			conn, err = d.Dial(c.Network, c.Addr)
		}
		if err != nil {
			return connSet, errors.Annotatef(err, "could not connect to balancer '%s'", c.Addr)
		}
		connSet = append(connSet, BalancerConn{
			Addr:    c.Addr,
			Network: c.Network,
			Conn:    conn,
		})
	}
	return connSet, nil
}

func (b *BalancerClient) sendCommand(connections []BalancerConn, cmd string) ([]string, error) {
	replies := make([]string, len(connections))
	for i, c := range connections {
		c.Conn.SetDeadline(time.Now().Add(b.Config.Timeout.Duration))
		log.Infof("[balancer] %s <- %s", c.Addr, cmd)
		_, err := c.Conn.Write([]byte(cmd))
		if err != nil {
			err = errors.Annotatef(err, "error sending command to balancer '%s'", c.Addr)
			log.Errorf("[balancer] %s", err)
			return replies, err
		}
		result := bytes.NewBuffer(nil)
		_, err = io.Copy(result, c.Conn)
		if err != nil {
			err = errors.Annotatef(err, "error reading reply from balancer '%s': %s", c.Addr, err)
			log.Errorf("[balancer] %s", err)
			return replies, err
		}
		replies[i] = result.String()
		log.Infof("[balancer] %s -> %s", c.Addr, replies[i])
	}
	return replies, nil
}

func (b *BalancerClient) disconnect(connections []BalancerConn) {
	for _, c := range connections {
		if c.Conn != nil {
			c.Conn.Close()
			c.Conn = nil
		}
	}
}

func (b *BalancerClient) doQuery(connections []BalancerConn, cmd string) ([]string, error) {
	connSet, err := b.connect(connections)
	if err != nil {
		b.disconnect(connSet)
		return nil, errors.Trace(err)
	}
	replies, err := b.sendCommand(connSet, cmd)
	b.disconnect(connSet)
	return replies, err
}

func showServersStateCommand() string {
	return "show servers state\n"
}

func setServerStateCommand(backend, server, state string) string {
	return fmt.Sprintf("set server %s/%s state %s\n", backend, server, state)
}
