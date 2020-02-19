package objectbucketclaim

import (
	"context"
	"fmt"
	"github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	"github.com/yard-turkey/cosi-prototype-driver/pkg/controller/flow_control"
	. "github.com/yard-turkey/cosi-prototype-driver/pkg/controller/objectbucketclaim/requestLogger"
	"github.com/yard-turkey/cosi-prototype-interface/cosi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// claimSyncWorker loosens controller-to-plugin coupling by making per-iteration connections to an OBC's plugin.  Doing
// so enables the controller to dial plugins on demand..  This differs from other common designs where a
// controller maintains a connection to a single plugin over its lifetime. The claimSyncWorker executes a Reconcile()
// iteration with the given family of API objects and is then discarded.
type claimSyncWorker struct {
	ctx    context.Context
	scheme *runtime.Scheme
	client client.Client

	// pluginClient must be Closed()
	pluginClient *pluginClient

	obc          *v1alpha1.ObjectBucketClaim
	class        *v1alpha1.BucketClass
	registration *v1alpha1.PluginRegistration
}

func (w *claimSyncWorker) Close() error {
	return w.pluginClient.Close()
}

func newClaimSyncWorker(c client.Client, ctx context.Context, obc *v1alpha1.ObjectBucketClaim, bc *v1alpha1.BucketClass, reg *v1alpha1.PluginRegistration) (*claimSyncWorker, error) {

	if reg.Status.CertSecret == nil {
		return nil, fmt.Errorf("plugin %s has not been issued a TLS certificate", reg.Spec.PluginName)
	}

	sec := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: reg.Status.CertSecret.Namespace, Name: reg.Status.CertSecret.Name}, sec)

	pc, err := newPluginClient(c, getTLSCertificate(sec), reg.Spec.Host)
	if err != nil {
		return nil, err
	}
	return &claimSyncWorker{
		client:       c,
		ctx:          ctx,
		pluginClient: pc,
		obc:          obc,
		class:        bc,
		registration: reg,
	}, nil
}

func getTLSCertificate(sec *corev1.Secret) []byte {
	return sec.Data["tls.crt"]
}

func (w *claimSyncWorker) handleProvisionClaim() error {

	err := w.lockOBC()
	if err != nil {
		return err
	}

	err = w.setClaimPhasePending()
	if err != nil {
		return err
	}

	resp, err := w.provision()
	if err != nil {
		return err
	}

	ob, err := w.createObjectBucket(resp)
	if err != nil {
		return err
	}

	err = w.setObjectBucketName(ob.Name)
	if err != nil {
		return err
	}

	_, err = w.createChildSecret(resp.GetEnvironmentCredentials())
	if err != nil {
		return err
	}
	_, err = w.createChildConfigMap(resp)
	if err != nil {
		return err
	}

	err = w.setClaimPhaseBound()
	if err != nil {
		return err
	}

	Debug.Info("provisioning succeeded")
	return nil
}

func (w *claimSyncWorker) provision() (*cosi.ProvisionResponse, error) {
	Debug.Info("provisioning bucket", "OBC", fmt.Sprintf("%s/%s", w.obc.Namespace, w.obc.Name))

	resp, err := w.pluginClient.Provision(w.ctx, &cosi.ProvisionRequest{
		RequestBucketName: w.obc.Spec.BucketName,
		Parameters:        getClassParameters(w.class),
	})
	if resp == nil && err == nil {
		// The pluginClient is not honoring the interface
		return nil, fmt.Errorf("pluginClient returned nil response and no error")
	}
	return resp, err
}

func (w *claimSyncWorker) handleDeprovisionClaim() error {
	Log.Info("deprovisioning bucket", "OBC", fmt.Sprintf("%s/%s", w.obc.Namespace, w.obc.Name))
	// TODO right now we ignore the response, the prototype plugin doesn't send anything meaningful
	_, err := w.pluginClient.Deprovision(w.ctx, &cosi.DeprovisionRequest{
		BucketName: w.obc.Spec.BucketName,
	})
	if err != nil {
		return err
	}

	err = w.deleteBoundObjectBucket()
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	err = w.unlockOBC()
	if err != nil {
		return err
	}

	Log.Info("deprovisioning succeeded")
	return nil
}

func (r *ReconcileObjectBucketClaim) bucketClassFromClaim(obc *v1alpha1.ObjectBucketClaim) (*v1alpha1.BucketClass, error) {
	Debug.Info("fetching bucketClass", "name", obc.Spec.BucketClassName, "namespace", obc.Spec.BucketClassNamespace)
	bc := &v1alpha1.BucketClass{}
	err := r.client.Get(r.ctx, client.ObjectKey{Namespace: obc.Spec.BucketClassNamespace, Name: obc.Spec.BucketClassName}, bc)
	return bc, err
}

func (w *claimSyncWorker) setClaimPhasePending() error {
	return w.setPhase(v1alpha1.ObjectBucketClaimStatusPhasePending)
}

func (w *claimSyncWorker) setClaimPhaseBound() error {
	return w.setPhase(v1alpha1.ObjectBucketClaimStatusPhaseBound)
}

func (w *claimSyncWorker) setPhase(p v1alpha1.ObjectBucketClaimStatusPhase) error {
	Debug.Info("setting claim phase", "new phase", p)
	w.obc.Status.Phase = p
	return w.client.Update(w.ctx, w.obc)
}

func (w *claimSyncWorker) setOBCBucketName(obc *v1alpha1.ObjectBucketClaim, bucket string) error {
	Debug.Info("setting obc.Spec.BucketName", "BucketName", obc.Spec.BucketName)
	obc.Spec.BucketName = bucket
	return w.client.Update(w.ctx, obc)
}

func (w *claimSyncWorker) setObjectBucketName(objectBucketName string) error {
	Debug.Info("setting obc.Spec.ObjectBucketName", "ObjectBucketName", objectBucketName)
	w.obc.Spec.ObjectBucketName = objectBucketName
	return w.client.Update(w.ctx, w.obc)
}

func (w *claimSyncWorker) createChildSecret(data map[string]string) (*corev1.Secret, error) {
	sec := generateSecret(w.obc, data)
	Debug.Info("creating child secret", "Namespace", sec.Namespace, "Name", sec.Name)
	err := controllerutil.SetControllerReference(w.obc, sec, w.scheme)
	if err != nil {
		return nil, err
	}
	err = w.client.Create(w.ctx, sec)
	return sec, err
}

func (w *claimSyncWorker) createChildConfigMap(resp *cosi.ProvisionResponse) (*corev1.ConfigMap, error) {
	cm := generateConfigMap(w.obc, resp)
	Debug.Info("creating child config map", "Namespace", cm.Namespace, "Name", cm.Name)
	// TODO push this call down in generate* calls
	err := controllerutil.SetControllerReference(w.obc, cm, w.scheme)
	if err != nil {
		return nil, err
	}
	err = w.client.Create(w.ctx, cm)
	return cm, err
}

func (w *claimSyncWorker) createObjectBucket(resp *cosi.ProvisionResponse) (*v1alpha1.ObjectBucket, error) {
	ob := generateObjectBucket(w.obc, resp, w.class.Spec.ReleasePolicy)
	Debug.Info("create object bucket", "Name", ob.Name)
	err := w.client.Create(w.ctx, ob)
	return ob, err
}

func (w *claimSyncWorker) deleteBoundObjectBucket() error {
	Debug.Info("deleting object bucket", "Name", w.obc.Spec.BucketName)
	if w.obc.Spec.ObjectBucketName == "" {
		return nil
	}
	ob := new(v1alpha1.ObjectBucket)
	err := w.client.Get(w.ctx, client.ObjectKey{Namespace: "", Name: w.obc.Spec.ObjectBucketName}, ob)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return w.client.Delete(w.ctx, ob)
}

func (w *claimSyncWorker) lockOBC() error {
	Debug.Info("locking claim")
	return flow_control.LockObject(w.client, w.ctx, w.obc)
}

func (w *claimSyncWorker) unlockOBC() error {
	Debug.Info("unlocking claim")
	return flow_control.UnLockObject(w.client, w.ctx, w.obc)
}

func generateSecret(obc *v1alpha1.ObjectBucketClaim, accessCredentials map[string]string) *corev1.Secret {
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
	// TODO (copejon) I'm thinking the plugin should define the env var and the driver just pass them through.
	// These hardcoded values are just for tire kicking the prototype
	cm.Data = map[string]string{
		"COSI_BUCKET_ENDPOINT": resp.Endpoint,
		"COSI_BUCKET_REGION":   resp.Region,
		"COSI_BUCKET_NAME":     resp.BucketName,
	}
	for k, v := range resp.Data {
		cm.Data[k] = v
	}
	return cm
}

// generateObjectBucket is messier than its cm and sec counterparts because nested structures are not allocated
// space by new(), so they must be declared inline.
func generateObjectBucket(obc *v1alpha1.ObjectBucketClaim, resp *cosi.ProvisionResponse, pol *v1alpha1.ObjectBucketReleasePolicy) *v1alpha1.ObjectBucket {
	ob := &v1alpha1.ObjectBucket{
		ObjectMeta: v12.ObjectMeta{
			Name:      childResourceName(fmt.Sprintf("%s-%s", obc.Namespace, obc.Name)),
			Namespace: obc.Namespace,
		},
		Spec: v1alpha1.ObjectBucketSpec{
			ReleasePolicy:        pol,
			BucketClassName:      obc.Spec.BucketClassName,
			BucketClassNamespace: obc.Spec.BucketClassNamespace,

			ClaimRef: makeObjectReference(obc),
			Connection: &v1alpha1.Connection{
				Endpoint: &v1alpha1.Endpoint{
					BucketHost:           resp.Endpoint,
					BucketPort:           0,
					BucketName:           resp.BucketName,
					Region:               resp.Region,
					AdditionalConfigData: map[string]string{},
				},
				Authentication: &v1alpha1.Authentication{
					AdditionalSecretData: resp.EnvironmentCredentials,
					AccessKeys:           &v1alpha1.AccessKeys{}, // TODO (copejon) should we let plugins decide the env var?
				},
				AdditionalState: map[string]string{}, // TODO (copejon) i'm ignoring this for now
			},
		},
		Status: v1alpha1.ObjectBucketStatus{Phase: v1alpha1.ObjectBucketStatusPhaseBound},
	}
	return ob
}

const prefix = "cosi.io"

func childResourceName(obcName string) string {
	return fmt.Sprintf("%s-%s", prefix, obcName)
}

func makeObjectReference(obj v12.Object) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       v1alpha1.ObjectBucketClaimGVK().Kind,
		Name:       obj.GetName(),
		Namespace:  obj.GetNamespace(),
		UID:        obj.GetUID(),
	}
}

// getClassParameters protects against uninitialized field access
func getClassParameters(bc *v1alpha1.BucketClass) map[string]string {
	Debug.Info(fmt.Sprintf("getting StorageClass parameters: %v", *bc))
	if bc.Spec.PluginParameters != nil {
		return bc.Spec.PluginParameters
	}
	return make(map[string]string)
}
