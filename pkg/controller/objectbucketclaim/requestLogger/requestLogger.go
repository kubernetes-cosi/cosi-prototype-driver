package requestLogger

import (
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var Log logr.Logger
var Debug logr.InfoLogger

const debugLevel = 1

func ResetLogger(req reconcile.Request){
	Log = log.Log.WithName(name).WithValues("Namespace", req.Namespace, "Name", req.Name)
	Debug = Log.V(debugLevel)
}

const name = "cosi.io"

func init(){
	Log = log.Log.WithName(name)
	Debug = Log.V(debugLevel)
}
