package objectbucketclaim

import (
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
	"google.golang.org/grpc"
	"os"
)

const (
	env_listen                = "COSI_SERVER_LISTEN"
	env_listen_default        = "localhost:8080"
)

// grpcClient Singleton. Wraps grpc connection channel and the Provisioner client
// so that callers may call Close() on the same instance as they may call
// provisioner client methods.  The connection and client are instanced at
// startup.  This gives a fail fast advantage on connection issues and
// will result in cleaner logs for debugging the issue.
var grpcClient = struct {
	cosi.ProvisionerClient
	*grpc.ClientConn
}{}


func init() {
	listen := env_listen_default
	if l, ok := os.LookupEnv(env_listen); ok {
		listen = l
	}
	conn, err := grpc.Dial(listen)
	if err != nil {
		panic(err)
	}

	grpcClient = struct {
		cosi.ProvisionerClient
		*grpc.ClientConn
	}{cosi.NewProvisionerClient(conn), conn}
}
