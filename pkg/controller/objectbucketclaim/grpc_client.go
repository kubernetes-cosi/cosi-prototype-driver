package objectbucketclaim

import (
	"context"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
	"google.golang.org/grpc"
	"os"
	"time"
)

const (
	ENV_LISTEN         = "COSI_GRPC_LISTEN"
	ENV_LISTEN_DEFAULT = "localhost:8080"
)

// grpcClient Singleton. Wraps grpc connection channel and the Provisioner client
// so that callers may call Close() on the same instance as they may call
// provisioner client methods.  The connection and client are instanced at
// startup.  This gives a fail fast advantage on connection issues and
// will result in cleaner logs for debugging the issue.
var grpcClient cosi.ProvisionerClient

func init() {
	listen := ENV_LISTEN_DEFAULT
	if l, ok := os.LookupEnv(ENV_LISTEN); ok {
		listen = l
	}
	// TODO (copejon) I think this will work to prevent server startup races.  Without it,
	//  the dial may occur before the server is up, resulting in a panic.
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := grpc.DialContext(ctx, listen, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	grpcClient = cosi.NewProvisionerClient(conn)
}
