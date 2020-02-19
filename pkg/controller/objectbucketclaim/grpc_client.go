package objectbucketclaim

import (
	"context"
	"crypto/x509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"

	. "github.com/yard-turkey/cosi-prototype-driver/pkg/controller/objectbucketclaim/requestLogger"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
)

// pluginClient is used to encapsulate an RPC connection and the ProvisionerClient interface.  The grpc.ClientConn
// must be closed and the CancelFunc must be called before the object is discarded.  It's recommended this be deferred
// immediately after instantiation.
type pluginClient struct {
	Client client.Client
	Ctx    context.Context
	*grpc.ClientConn
	cosi.ProvisionerClient
}

func newPluginClient(c client.Client, pem []byte, endpoint string) (*pluginClient, error) {
	Debug.Info("creating plugin Client")
	p := &pluginClient{Client: c, Ctx: context.Background()}

	creds := credentialsFromCert(pem)
	conn, err := dialRPCListener(p.Ctx, endpoint, creds)
	if err != nil {
		return nil, err
	}

	p.ClientConn = conn
	p.ProvisionerClient = cosi.NewProvisionerClient(conn)
	return p, nil
}

// dialTimeout is the upper time limit to allow for establishing a TCP connection.  If the plugin cannot be dialed in
// this time, the dial is canceled.
const dialTimeout = 30 * time.Second

// dialRPCListener will block for no longer than dialTimeout for a connection to be made. This will work to prevent
// server startup races.  Without it, the dial may occur before the server is up, resulting in a panic.
func dialRPCListener(ctx context.Context, listen string, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	ctxTimetout, _ := context.WithTimeout(ctx, dialTimeout) //
	Debug.Info("dialing plugin server")

	conn, err := grpc.DialContext(ctxTimetout, listen, grpc.WithTransportCredentials(creds), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func credentialsFromCert(pem []byte) credentials.TransportCredentials {
	Debug.Info("parsing plugin CertificateSigningRequest certificate")
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return credentials.NewClientTLSFromCert(pool, "")
}
