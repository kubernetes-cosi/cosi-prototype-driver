package objectbucketclaim

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	. "github.com/yard-turkey/cosi-prototype-driver/pkg/controller/objectbucketclaim/requestLogger"
)

// Add creates a new ObjectBucketClaim Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	ctx := context.Background()

	return &ReconcileObjectBucketClaim{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		ctx:    ctx,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("objectbucketclaim-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource ObjectBucketClaim
	err = c.Watch(&source.Kind{Type: &v1alpha1.ObjectBucketClaim{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Pods and requeue the owner ObjectBucketClaim
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &v1alpha1.ObjectBucketClaim{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileObjectBucketClaim implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileObjectBucketClaim{}

// ReconcileObjectBucketClaim reconciles a ObjectBucketClaim object
type ReconcileObjectBucketClaim struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme

	// ctx is the parent context of child timeout contexts used to regulate grpclient method
	// calls under the Reconcile() call stack.
	ctx context.Context
}

// Reconcile reads that state of the cluster for a ObjectBucketClaim object and makes changes based on the state read
// and what is in the ObjectBucketClaim.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileObjectBucketClaim) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ResetLogger(request)
	Log.Info("Reconciling new request")

	// Fetch the ObjectBucketClaim instance
	Debug.Info("fetching request OBC")
	instance := &v1alpha1.ObjectBucketClaim{}
	err := r.client.Get(r.ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// If the OBC doesn't exist, it was either deleted before Provisioning began or was garbage collected after
			// a successful deprovision.  In both cases, no further action is required.
			Debug.Info("OBC not found, assuming it was deleted before resources were provisioned or children were already cleaned up")
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	err = r.syncClaim(instance)

	return reconcile.Result{}, err
}

func (r *ReconcileObjectBucketClaim) syncClaim(obc *v1alpha1.ObjectBucketClaim) error {
	Log.Info("syncing claim")

	worker, err := r.newConfiguredSyncClaimWorker(obc)
	if err != nil {
		return err
	}
	defer worker.Close()

	if isDeletionEvent(obc) {
		Debug.Info("processing deletion")
		err = worker.handleDeprovisionClaim()
	} else {
		// Interruptions in provisioning may result in an actual state of the world where the OB was not set in the
		// OBC but the secret and config map were created.  We cannot short circuit syncClaim by checking
		// this field earlier as deletions may still need to clean up artifacts.
		if pendingProvisioning(obc) {
			//By now, we should know that the OBC matches our plugin, lacks an OB, and thus requires provisioning
			err = worker.handleProvisionClaim()
		} else {
			Log.Info("obc already fulfilled, skipping")
		}
	}

	return err
}

func (r *ReconcileObjectBucketClaim) newConfiguredSyncClaimWorker(obc *v1alpha1.ObjectBucketClaim) (*claimSyncWorker, error) {
	bc, err := r.bucketClassFromClaim(obc)
	if err != nil {
		return nil, err
	}

	reg, err := r.getPluginRegistrationFromClass(bc)
	if err != nil {
		return nil, err
	}

	if reg.Status.CertSecret == nil {
		return nil, fmt.Errorf("plugin %s has not been issued a TLS cert", reg.Spec.PluginName)
	}

	w, err := newClaimSyncWorker(r.client, r.ctx, obc, bc, reg)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (r *ReconcileObjectBucketClaim) getPluginRegistrationFromClass(class *v1alpha1.BucketClass) (*v1alpha1.PluginRegistration, error) {
	Debug.Info("getting plugin registration")
	reg := &v1alpha1.PluginRegistration{}
	err := r.client.Get(r.ctx, client.ObjectKey{Namespace: class.Spec.PluginRegistrationNamespace, Name: class.Spec.PluginRegistrationName}, reg)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

// pendingProvisioning detects if an OB name is set on the OBC.  If so, assume provisioning was
// already completed.
func pendingProvisioning(obc *v1alpha1.ObjectBucketClaim) bool {
	return obc.Spec.ObjectBucketName == ""
}

func isDeletionEvent(obc *v1alpha1.ObjectBucketClaim) bool {
	return obc.DeletionTimestamp != nil
}
