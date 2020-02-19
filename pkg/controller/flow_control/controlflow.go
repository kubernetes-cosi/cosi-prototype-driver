package flow_control

import (
	"context"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	finalizer = `cosi.io/finalizer`
)

func LockObject(c client.Client, ctx context.Context, obj runtime.Object) (err error) {
	err = controllerutil.AddFinalizerWithError(obj, finalizer)
	if err != nil {
		return
	}
	err = c.Update(ctx, obj)
	if err != nil {
		return
	}
	return
}

func UnLockObject(c client.Client, ctx context.Context, obj runtime.Object) (err error) {
	err = controllerutil.RemoveFinalizerWithError(obj, finalizer)
	if err != nil {
		return
	}
	err = c.Update(ctx, obj)
	return
}
