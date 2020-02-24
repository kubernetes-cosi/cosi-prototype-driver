package pluginregistration

import (
	"context"
	"github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	"github.com/yard-turkey/cosi-prototype-driver/pkg/controller/flow_control"
	tlsutil "github.com/yard-turkey/cosi-prototype-driver/pkg/tls_util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_plugin_registration")

// Add creates a new PluginRegistration Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcilePluginRegistration{client: mgr.GetClient(), scheme: mgr.GetScheme(), ctx: context.Background()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("pluginregistration-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource PluginRegistration
	err = c.Watch(&source.Kind{Type: &v1alpha1.PluginRegistration{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner PluginRegistration
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &v1alpha1.PluginRegistration{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcilePluginRegistration implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcilePluginRegistration{}

// ReconcilePluginRegistration reconciles a PluginRegistration object
type ReconcilePluginRegistration struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	ctx    context.Context
}

// Reconcile reads that state of the cluster for a PluginRegistration object and makes changes based on the state read
// and what is in the PluginRegistration.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcilePluginRegistration) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling PluginRegistration")

	// Fetch the PluginRegistration instance
	instance := &v1alpha1.PluginRegistration{}
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

	if isDeleted(instance) {
		if err = r.revoke(instance); err != nil {
			return reconcile.Result{}, err
		}
		err = r.UnlockRegistration(instance)
	} else if needsCertificate(instance) {
		// Ignores cases where the key was an Update or Add occurred on a processed cert
		if err = r.issue(instance); err != nil {
			return reconcile.Result{}, err
		}
		err = r.LockRegistration(instance)
	}

	return reconcile.Result{}, nil
}

func (r *ReconcilePluginRegistration) issue(registration *v1alpha1.PluginRegistration) error {
	cert, key, err := tlsutil.GeneratePluginServerChildCert(registration.Spec.Host)
	if err != nil {
		return err
	}
	return r.issueSecretCredential(tlsutil.PEMSerializedCertificate(cert), tlsutil.PemSerializedRSAKey(key), registration)
}

func (r *ReconcilePluginRegistration) createNewCertSecret(namespace, name string, cert, key []byte) (*corev1.Secret, error) {
	sec := newTLSSecret(namespace, name, cert, key)
	err := r.client.Create(r.ctx, sec)
	if err != nil {
		return nil, err
	}
	return sec, nil
}

func (r ReconcilePluginRegistration) issueSecretCredential(cert, key []byte, registration *v1alpha1.PluginRegistration) error {
	sec, err := r.createNewCertSecret(registration.Namespace, registration.Name, cert, key)
	if err != nil {
		return err
	}
	return r.registerSecret(registration, sec)
}

func newTLSSecret(namespace, name string, cert, key []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type: "kubernetes.io/tls",
		Data: map[string][]byte{
			"tls.crt": cert,
			"tls.key": key,
		},
	}
}

func (r *ReconcilePluginRegistration) registerSecret(registration *v1alpha1.PluginRegistration, secret *corev1.Secret) error {
	registration.Status.CertSecret = secretReference(secret)
	return r.client.Status().Update(r.ctx, registration)
}

func secretReference(s *corev1.Secret) *corev1.SecretReference {
	return &corev1.SecretReference{
		Name:      s.Name,
		Namespace: s.Namespace,
	}
}

func (r *ReconcilePluginRegistration) revoke(registration *v1alpha1.PluginRegistration) error {
	sec, err := r.getRegisteredSecret(registration)
	// if the secret does not exist, ignore the error and continue cleaning up
	if err != nil && ! errors.IsNotFound(err) {
		return err
	}
	if sec != nil {
		if err = r.client.Delete(r.ctx, sec); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReconcilePluginRegistration) getRegisteredSecret(registration *v1alpha1.PluginRegistration) (*corev1.Secret, error) {
	sec := new(corev1.Secret)
	key, err := client.ObjectKeyFromObject(registration)
	if err != nil {
		return nil, err
	}
	err = r.client.Get(r.ctx, key, sec)
	return sec, err
}

func (r *ReconcilePluginRegistration) LockRegistration(reg *v1alpha1.PluginRegistration) error {
	return flow_control.LockObject(r.client, r.ctx, reg)
}

func (r *ReconcilePluginRegistration) UnlockRegistration(reg *v1alpha1.PluginRegistration) error {
	return flow_control.UnLockObject(r.client, r.ctx, reg)
}

func needsCertificate(reg *v1alpha1.PluginRegistration) bool {
	return reg.Status.CertSecret == nil
}

func isDeleted(reg *v1alpha1.PluginRegistration) bool {
	return reg.GetDeletionTimestamp() != nil
}
