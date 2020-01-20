package objectbucketclaim

import (
	"context"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
	storagev1 "k8s.io/api/storage/v1"

	objectbucketiov1alpha1 "github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_objectbucketclaim")

// Add creates a new ObjectBucketClaim Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileObjectBucketClaim{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("objectbucketclaim-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource ObjectBucketClaim
	err = c.Watch(&source.Kind{Type: &objectbucketiov1alpha1.ObjectBucketClaim{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner ObjectBucketClaim
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &objectbucketiov1alpha1.ObjectBucketClaim{},
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
	pluginName string
}

// Reconcile reads that state of the cluster for a ObjectBucketClaim object and makes changes based on the state read
// and what is in the ObjectBucketClaim.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileObjectBucketClaim) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling ObjectBucketClaim")

	// Fetch the ObjectBucketClaim instance
	instance := &objectbucketiov1alpha1.ObjectBucketClaim{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	err = r.syncHandler(instance)

	//reqLogger.Info("Skip reconcile: Pod already exists", "Pod.Namespace", found.Namespace, "Pod.Name", found.Name)
	return reconcile.Result{}, nil
}

// Reconcile implements the Reconciler interface. This function contains the business logic
// of the OBC obcController.
// Note: the obc obtained from the key is not expected to be nil. In other words, this func is
//   not called when informers detect an object is missing and trigger a formal delete event.
//   Instead, delete is indicated by the deletionTimestamp being non-nil on an update event.
func (r *ReconcileObjectBucketClaim) syncHandler(obc *objectbucketiov1alpha1.ObjectBucketClaim) error {

	scKey := client.ObjectKey{"", obc.Spec.StorageClassName}

	storageClassInstance := &storagev1.StorageClass{}
	err := r.client.Get(context.TODO(), scKey, storageClassInstance)
	if err != nil {
		log.Error(err, "storage class not found")
	}

	if !r.isSupportedPlugin(storageClassInstance.Provisioner) {
		log.Info("unsupported provisioner", "got", storageClassInstance.Provisioner)
		return nil
	}

	// ***********************
	// Delete or Revoke Bucket
	// ***********************
	if obc.ObjectMeta.DeletionTimestamp != nil {
		log.Info("OBC deleted, proceeding with cleanup")
		//return c.handleDeleteClaim(key, obc)
	}

	// *******************************************************
	// Provision New Bucket or Grant Access to Existing Bucket
	// *******************************************************
	if !shouldProvision(obc) {
		log.Info("skipping provision")
		return nil
	}

	// update the OBC's status to pending before any provisioning related errors can occur
	//obc, err = updateObjectBucketClaimPhase(
	//	c.libClientset,
	//	obc,
	//	v1alpha1.ObjectBucketClaimStatusPhasePending,
	//	defaultRetryBaseInterval,
	//	defaultRetryTimeout)
	//if err != nil {
	//	return fmt.Errorf("error updating OBC status: %s", err)
	//}

	// By now, we should know that the OBC matches our provisioner, lacks an OB, and thus requires provisioning
	//err = c.handleProvisionClaim(key, obc, class)
	resp, err := grpcClient.Provision(context.TODO(), &cosi.ProvisionRequest{})
	log.Info("got response", "ProvisionerResponse", *resp)

	// If handleReconcile() errors, the request will be re-queued.  In the distant future, we will likely want some ignorable error types in order to skip re-queuing
	return err
}

func (r *ReconcileObjectBucketClaim) isSupportedPlugin(name string) bool {
	return r.pluginName == name
}
