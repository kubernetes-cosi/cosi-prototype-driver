package objectbucketclaim

import (
	"context"
	"google.golang.org/grpc"
	"os"
	"time"

	. "github.com/yard-turkey/cosi-prototype-driver/pkg/controller/objectbucketclaim/requestLogger"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
)

const (
	ENV_LISTEN    = "COSI_GRPC_LISTEN"
	listenDefault = "localhost:8080"
)

// grpcClient Singleton. Wraps grpc connection channel and the Provisioner client
// so that callers may call Close() on the same instance as they may call
// provisioner client methods.  The connection and client are instanced at
// startup.  This gives a fail fast advantage on connection issues and
// will result in cleaner logs for debugging the issue.
var grpcClient cosi.ProvisionerClient

func init() {
	Debug.Info("initializing grpc client")
	listen := listenDefault
	if l, ok := os.LookupEnv(ENV_LISTEN); ok {
		listen = l
	}
	Debug.Info("grpc client endpoint: " + listen)
	// This will work to prevent server startup races.  Without it, the dial may occur before the server is up,
	// resulting in a panic.
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	Debug.Info("connecting to plugin server")
	conn, err := grpc.DialContext(ctx, listen, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	Debug.Info("creating provisioner client")
	grpcClient = cosi.NewProvisionerClient(conn)
}
