package objectbucketclaim

import (
	"context"
	"fmt"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
)

var log = logf.Log.WithName("controller_objectbucketclaim")
var debug = log.V(1)

// Add creates a new ObjectBucketClaim Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

const pluginName = "cosi-tester" // TODO this needs to be defined in the plugin and communicated here via rpc

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileObjectBucketClaim{
		client:     mgr.GetClient(),
		scheme:     mgr.GetScheme(),
		pluginName: pluginName,
		ctx:        context.Background(),
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

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
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
	client     client.Client
	scheme     *runtime.Scheme
	pluginName string

	// ctx is the parent context of child timeout contexts used to regulate grpclient method
	// calls under the Reconcile() call stack.
	ctx context.Context
	//
	timeoutCtx context.Context
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
	instance := &v1alpha1.ObjectBucketClaim{}
	err := r.client.Get(r.ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	log.Info("DEBUG starting syncClaim")
	err = r.syncClaim(instance)

	//reqLogger.Info("Skip reconcile: Pod already exists", "Pod.Namespace", found.Namespace, "Pod.Name", found.Name)
	return reconcile.Result{}, err
}

const requestTimeout = 30 * time.Second

func (r *ReconcileObjectBucketClaim) syncClaim(obc *v1alpha1.ObjectBucketClaim) error {

	// timeoutCtx is a child of r.ctx. It is killed either on timeout or when cancelFunc is called.  Thus, it must be
	// recreated every sync iteration and cannot be reused.
	timeoutCtx, cancelFunc := context.WithTimeout(r.ctx, requestTimeout)
	// TODO placeholder, not sure if we'll need the cancelFunc.  Probably not as (de)provisioning isn't an async op
	var _ = cancelFunc

	storageClassInstance, err := r.storageClassFromClaim(obc)
	if err != nil {
		return err
	}
	if r.isSupportedPlugin(storageClassInstance.Provisioner) {
		// ***********************
		// Delete
		// ***********************
		if isDeletionEvent(obc) {
			debug.Info("processing deletion")
			err = r.handleDeprovisionClaim(timeoutCtx, obc)
		} else {
			// Interruptions in provisioning may result in an actual state of the world where the OB was not set in the
			// OBC but the secret and config map were created.  So we cannot short circuit syncClaim by checking
			// this field earlier as deletions may still need to clean up artifacts.
			if pendingProvisioning(obc) {
				// *******************************************************
				// Provision New Bucket
				// *******************************************************

				//By now, we should know that the OBC matches our plugin, lacks an OB, and thus requires provisioning
				err = r.handleProvisionClaim(timeoutCtx, obc, storageClassInstance)
			} else {
				log.Info("obc already fulfilled, skipping")
			}
		}
	}

	// If handleReconcile() errors, the request will be re-queued.  In the distant future, we will likely want some ignorable error types in order to skip re-queuing
	return err
}

func (r *ReconcileObjectBucketClaim) handleProvisionClaim(timeoutCtx context.Context, obc *v1alpha1.ObjectBucketClaim, sc *storagev1.StorageClass) error {

	// Errors caused by existing resources indicates this is a retry on a partially successful sync (probably?)
	// Name collisions are controlled because they are derived from OBCs.  An OBC name collision would be caught by the
	// api server.
	isFatalError := func(e error) bool { return e != nil && ! apierrs.IsAlreadyExists(e) }

	debug.Info("locking objectBucketClaim")
	err := r.lockObject(obc)
	if isFatalError(err) {
		return err
	}

	debug.Info("updating bucket phase", "phase", v1alpha1.ObjectBucketClaimStatusPhasePending)
	err = r.setClaimPhasePending(obc)
	if isFatalError(err) {
		return err
	}

	// TODO pass SC parameters
	debug.Info("provisioning bucket", "OBC", fmt.Sprintf("%s/%s", obc.Namespace, obc.Name))
	resp, err := grpcClient.Provision(timeoutCtx, &cosi.ProvisionRequest{
		RequestBucketName: obc.Spec.BucketName,
	})
	if isFatalError(err) {
		return err
	}

	ob, err := r.newObjectBucket(obc, resp, sc.ReclaimPolicy)
	if isFatalError(err) {
		return err
	}

	err = r.setObjectBucketName(obc, ob.Name)
	if isFatalError(err) {
		return err
	}

	_, err = r.createChildSecret(obc, resp.GetEnvironmentCredentials())
	if isFatalError(err) {
		return err
	}
	_, err = r.createChildConfigMap(obc, resp)
	if isFatalError(err) {
		return err
	}

	err = r.setClaimPhaseBound(obc)
	if isFatalError(err) {
		return err
	}

	debug.Info("provisioning succeeded", "plugin response", resp)
	return nil
}

func (r *ReconcileObjectBucketClaim) handleDeprovisionClaim(timeoutCtx context.Context, obc *v1alpha1.ObjectBucketClaim) error {
	debug.Info("deprovisioning bucket", "OBC", fmt.Sprintf("%s/%s", obc.Namespace, obc.Name))
	resp, err := grpcClient.Deprovision(timeoutCtx, &cosi.DeprovisionRequest{
		BucketName: obc.Spec.BucketName,
	})
	if err != nil {
		return err
	}

	err = r.unlockObjectBucketClaim(obc)
	if err != nil {
		return err
	}

	debug.Info("deprovisioning succeeded", "plugin response", resp)
	return nil
}

func (r *ReconcileObjectBucketClaim) isSupportedPlugin(name string) bool {
	match := r.pluginName == name
	if ! match {
		log.Info("this OBC is not managed by this provisioner")
	}
	return match
}

func (r *ReconcileObjectBucketClaim) storageClassFromClaim(obc *v1alpha1.ObjectBucketClaim) (*storagev1.StorageClass, error) {
	sc := &storagev1.StorageClass{}
	err := r.client.Get(r.ctx, client.ObjectKey{"", obc.Spec.StorageClassName}, sc)
	return sc, err
}

func (r *ReconcileObjectBucketClaim) setClaimPhasePending(obc *v1alpha1.ObjectBucketClaim) error {
	return r.setPhase(obc, v1alpha1.ObjectBucketClaimStatusPhasePending)
}

func (r *ReconcileObjectBucketClaim) setClaimPhaseBound(obc *v1alpha1.ObjectBucketClaim) error {
	return r.setPhase(obc, v1alpha1.ObjectBucketClaimStatusPhaseBound)
}

func (r *ReconcileObjectBucketClaim) setPhase(obc *v1alpha1.ObjectBucketClaim, p v1alpha1.ObjectBucketClaimStatusPhase) error {
	obc.Status.Phase = p
	return r.client.Update(r.ctx, obc)
}

func (r *ReconcileObjectBucketClaim) setOBCBucketName(obc *v1alpha1.ObjectBucketClaim, bucket string) error {
	obc.Spec.BucketName = bucket
	return r.client.Update(r.ctx, obc)
}

func (r *ReconcileObjectBucketClaim) setObjectBucketName(obc *v1alpha1.ObjectBucketClaim, objectBucketName string) error {
	obc.Spec.ObjectBucketName = objectBucketName
	return r.client.Update(r.ctx, obc)
}

func (r *ReconcileObjectBucketClaim) createChildSecret(obc *v1alpha1.ObjectBucketClaim, accessCredentials map[string]string) (*corev1.Secret, error) {
	sec := generateSecret(obc, accessCredentials)
	err := controllerutil.SetControllerReference(obc, sec, r.scheme)
	if err != nil {
		return nil, err
	}
	err = r.client.Create(r.ctx, sec)
	return sec, err
}

func (r *ReconcileObjectBucketClaim) createChildConfigMap(obc *v1alpha1.ObjectBucketClaim, resp *cosi.ProvisionResponse) (*corev1.ConfigMap, error) {
	cm := generateConfigMap(obc, resp)
	err := controllerutil.SetControllerReference(obc, cm, r.scheme)
	if err != nil {
		return nil, err
	}
	err = r.client.Create(r.ctx, cm)
	return cm, err
}

func (r *ReconcileObjectBucketClaim) newObjectBucket(obc *v1alpha1.ObjectBucketClaim, resp *cosi.ProvisionResponse, reclaimPolicy *corev1.PersistentVolumeReclaimPolicy) (*v1alpha1.ObjectBucket, error) {
	ob := generateObjectBucket(obc, resp, reclaimPolicy)
	err := r.client.Create(r.ctx, ob)
	return ob, err
}

const objectBucketFinalizer = "cosi.io/finalizer"

func (r *ReconcileObjectBucketClaim) lockObject(obj runtime.Object) error {
	err := controllerutil.AddFinalizerWithError(obj, objectBucketFinalizer)
	if err != nil {
		return err
	}
	return r.client.Update(r.ctx, obj)
}

func (r *ReconcileObjectBucketClaim) unlockObjectBucketClaim(obj runtime.Object) error {
	err := controllerutil.RemoveFinalizerWithError(obj, objectBucketFinalizer)
	if err != nil {
		return err
	}
	return r.client.Update(r.ctx, obj)
}

func generateSecret(obc *v1alpha1.ObjectBucketClaim, accessCredentials map[string]string) (*corev1.Secret) {
	sec := new(corev1.Secret)
	sec.SetName(childResourceName(obc.Name))
	sec.SetNamespace(obc.Namespace)
	sec.StringData = accessCredentials
	return sec
}

func generateConfigMap(obc *v1alpha1.ObjectBucketClaim, resp *cosi.ProvisionResponse) *corev1.ConfigMap {
	cm := new(corev1.ConfigMap)
	cm.SetName(childResourceName(obc.Name))
	cm.SetNamespace(obc.Namespace)
	// TODO (copejon) I'm thinking the plugin should define the env var and the driver just pass them through
	cm.Data = map[string]string{
		"COSI_BUCKET_ENDPOINT": resp.Endpoint,
		"COSI_BUCKET_REGION": resp.Region,
		"COSI_BUCKET_NAME": resp.BucketName,
	}
	return cm
}

// generateObjectBucket is messier than its cm and sec counterparts because nested structures are not allocated
// space by new(), so they must be declared inline.
func generateObjectBucket(obc *v1alpha1.ObjectBucketClaim, resp *cosi.ProvisionResponse, pol *corev1.PersistentVolumeReclaimPolicy) *v1alpha1.ObjectBucket {
	ob := &v1alpha1.ObjectBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childResourceName(fmt.Sprintf("%s-%s", obc.Namespace, obc.Name)),
			Namespace: obc.Namespace,
		},
		Spec: v1alpha1.ObjectBucketSpec{
			ReclaimPolicy:    pol,
			StorageClassName: obc.Spec.StorageClassName,
			ClaimRef:         &corev1.ObjectReference{},
			Connection: &v1alpha1.Connection{
				Endpoint: &v1alpha1.Endpoint{
					BucketHost: resp.Endpoint,
					BucketPort: 0,
					BucketName: resp.BucketName,
					Region:     resp.Region,
					AdditionalConfigData: map[string]string{},
				},
				Authentication: &v1alpha1.Authentication{
					AdditionalSecretData: resp.EnvironmentCredentials,
					AccessKeys: &v1alpha1.AccessKeys{}, // TODO (copejon) should we let plugins decide the env var?
				},
				AdditionalState: map[string]string{}, // TODO (copejon) i'm ignoring this for now
			},
		},
	}
	return ob
}

const prefix = "cosi.io"

func childResourceName(obcName string) string {
	return fmt.Sprintf("%s-%s", prefix, obcName)
}

// pendingProvisioning detects if an OB name is set on the OBC.  If so, assume provisioning was
// already completed.
func pendingProvisioning(obc *v1alpha1.ObjectBucketClaim) bool {
	if obc.Spec.ObjectBucketName == "" {
		return true
	}
	log.Info("provisioning completed, skipping", "ObjectBucket", obc.Spec.ObjectBucketName)
	return false
}

func isDeletionEvent(obc *v1alpha1.ObjectBucketClaim) bool {
	return obc.DeletionTimestamp != nil
}
