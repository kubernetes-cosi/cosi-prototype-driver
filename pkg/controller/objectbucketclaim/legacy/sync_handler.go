// +build ignore

package legacy

import (
	"fmt"

	// TODO (copejon) move or generate the kube-object-storage packages in this project
	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/apis/objectbucket.io/v1alpha1"
	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/client/clientset/versioned"
	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/provisioner/api"
	pErr "github.com/kube-object-storage/lib-bucket-provisioner/pkg/provisioner/api/errors"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Provisioner is a CRD Controller responsible for executing the Reconcile() function
// in response to OBC events.
type obcController struct {
	clientset    kubernetes.Interface
	libClientset versioned.Interface
	// static label containing provisioner name and provisioner-specific labels which are all added
	// to the OB, OBC, configmap and secret
	provisionerLabels map[string]string
	provisioner       api.Provisioner
	provisionerName   string
}

// handleProvision is an extraction of the core provisioning process in order to defer clean up
// on a provisioning failure
func (c *obcController) handleProvisionClaim(key string, obc *v1alpha1.ObjectBucketClaim, class *storagev1.StorageClass) error {

	log.Info("syncing obc creation")

	var (
		ob        *v1alpha1.ObjectBucket
		secret    *corev1.Secret
		configMap *corev1.ConfigMap
		err       error
	)

	// set finalizer in OBC so that resources cleaned up is controlled when the obc is deleted
	if err = c.setOBCMetaFields(obc); err != nil {
		return err
	}

	// If a storage class contains a non-nil value for the "bucketName" key, it is assumed
	// to be a Grant request to the given bucket (brownfield).  If the value is nil or the
	// key is undefined, it is assumed to be a provisioning request.  This allows administrators
	// to control access to static buckets via RBAC rules on storage classes.
	isDynamicProvisioning := isNewBucketByStorageClass(class)

	// Should an error be returned, attempt to clean up the object store and API servers by
	// calling the appropriate provisioner method.  In cases where Provision() or Revoke()
	// return an err, it's likely that the ob == nil, hindering cleanup.
	defer func() {
		if err != nil && ob != nil {
			log.Info("cleaning up provisioning artifacts")
			if /*greenfield*/ isDynamicProvisioning && !pErr.IsBucketExists(err) {
				log.Info("deleting provisioned resources")
				if dErr := c.provisioner.Delete(ob); dErr != nil {
					log.Error(dErr, "could not delete provisioned resources")
				}
			} else /*brownfield*/ {
				log.Info("revoking access")
				if dErr := c.provisioner.Revoke(ob); dErr != nil {
					log.Error(err, "could not revoke access")
				}
			}
			_ = c.deleteResources(ob, configMap, secret, nil)
		}
	}()

	bucketName := class.Parameters[v1alpha1.StorageClassBucket]
	if isDynamicProvisioning {
		bucketName, err = composeBucketName(obc)
		if err != nil {
			return fmt.Errorf("error composing bucket name: %v", err)
		}
	}
	if len(bucketName) == 0 {
		return fmt.Errorf("bucket name missing")
	}

	// Re-Get the claim in order to shorten the race condition where the claim was deleted after Reconcile() started
	//obc, err = claimForKey(key, c.libClientset)
	//if err != nil {
	//	if errors.IsNotFound(err) {
	//		return fmt.Errorf("OBC was lost before we could provision: %v", err)
	//	}
	//	return err
	//}

	options := &api.BucketOptions{
		ReclaimPolicy:     class.ReclaimPolicy,
		BucketName:        bucketName,
		ObjectBucketClaim: obc.DeepCopy(),
		Parameters:        class.Parameters,
	}

	verb := "provisioning"
	if !isDynamicProvisioning {
		verb = "granting access to"
	}
	log.Info(verb, "bucket", options.BucketName)

	if isDynamicProvisioning {
		ob, err = c.provisioner.Provision(options)
	} else {
		ob, err = c.provisioner.Grant(options)
	}
	if err != nil {
		return fmt.Errorf("error %s bucket: %v", verb, err)
	} else if ob == (&v1alpha1.ObjectBucket{}) {
		return fmt.Errorf("provisioner returned nil/empty object bucket")
	}

	// create Secret and ConfigMap
	secret, err = createSecret(
		obc,
		ob.Spec.Authentication,
		c.provisionerLabels,
		c.clientset,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error creating secret for OBC: %v", err)
	}
	configMap, err = createConfigMap(
		obc,
		ob.Spec.Endpoint,
		c.provisionerLabels,
		c.clientset,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error creating configmap for OBC: %v", err)
	}

	// Create OB
	// Note: do not move ob create/update calls before secret or vice versa.
	//   spec.Authentication is lost after create/update, which break secret creation
	setObjectBucketName(ob, key)
	ob.Spec.StorageClassName = obc.Spec.StorageClassName
	//ob.Spec.ClaimRef, err = claimRefForKey(key, c.libClientset)
	ob.Spec.ReclaimPolicy = options.ReclaimPolicy
	ob.SetFinalizers([]string{finalizer})
	ob.SetLabels(c.provisionerLabels)

	ob, err = createObjectBucket(
		ob,
		c.libClientset,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error creating OB %q: %v", ob.Name, err)
	}
	ob, err = updateObjectBucketPhase(
		c.libClientset,
		ob,
		v1alpha1.ObjectBucketStatusPhaseBound,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error updating OB %q's status to %q: %v", ob.Name, v1alpha1.ObjectBucketStatusPhaseBound, err)
	}

	// update OBC
	obc.Spec.ObjectBucketName = ob.Name
	obc.Spec.BucketName = bucketName
	obc, err = updateClaim(
		c.libClientset,
		obc,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error updating OBC: %v", err)
	}
	obc, err = updateObjectBucketClaimPhase(
		c.libClientset,
		obc,
		v1alpha1.ObjectBucketClaimStatusPhaseBound,
		defaultRetryBaseInterval,
		defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error updating OBC %q's status to: %v", v1alpha1.ObjectBucketClaimStatusPhaseBound, err)
	}

	log.Info("provisioning succeeded")
	return nil
}

// Delete or Revoke access to bucket defined by passed-in key and obc.
func (c *obcController) handleDeleteClaim(key string, obc *v1alpha1.ObjectBucketClaim) error {
	// Call `Delete` for new (greenfield) buckets with reclaimPolicy == "Delete".
	// Call `Revoke` for new buckets with reclaimPolicy != "Delete".
	// Call `Revoke` for existing (brownfield) buckets regardless of reclaimPolicy.

	log.Info("syncing obc deletion")

	ob, cm, secret, errs := c.getExistingResourcesFromKey(key)
	if len(errs) > 0 {
		return fmt.Errorf("error getting resources: %v", errs)
	}

	// Delete/Revoke cannot be called if the ob is nil; however, if the secret
	// and/or cm != nil we can delete them
	if ob == nil {
		log.Error(nil, "nil ObjectBucket, assuming it has been deleted")
		return c.deleteResources(nil, cm, secret, obc)
	}

	if ob.Spec.ReclaimPolicy == nil {
		log.Error(nil, "missing reclaimPolicy", "ob", ob.Name)
		return nil
	}

	// call Delete or Revoke and then delete generated k8s resources
	// Note: if Delete or Revoke return err then we do not try to delete resources
	ob, err := updateObjectBucketPhase(c.libClientset, ob, v1alpha1.ObjectBucketClaimStatusPhaseReleased, defaultRetryBaseInterval, defaultRetryTimeout)
	if err != nil {
		return err
	}

	// decide whether Delete or Revoke is called
	if isNewBucketByObjectBucket(c.clientset, ob) && *ob.Spec.ReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
		if err = c.provisioner.Delete(ob); err != nil {
			// Do not proceed to deleting the ObjectBucket if the deprovisioning fails for bookkeeping purposes
			return fmt.Errorf("provisioner error deleting bucket %v", err)
		}
	} else {
		if err = c.provisioner.Revoke(ob); err != nil {
			return fmt.Errorf("provisioner error revoking access to bucket %v", err)
		}
	}

	return c.deleteResources(ob, cm, secret, obc)
}

// trim the errors resulting from objects not being found
func (c *obcController) getExistingResourcesFromKey(key string) (*v1alpha1.ObjectBucket, *corev1.ConfigMap, *corev1.Secret, []error) {
	ob, cm, secret, errs := c.getResourcesFromKey(key)
	for i := len(errs) - 1; i >= 0; i-- {
		if errors.IsNotFound(errs[i]) {
			errs = append(errs[:i], errs[i+1:]...)
		}
	}
	return ob, cm, secret, errs
}

// Gathers resources by names derived from key.
// Returns pointers to those resources if they exist, nil otherwise and an slice of errors who's
// len() == n errors. If no errors occur, len() is 0.
func (c *obcController) getResourcesFromKey(key string) (ob *v1alpha1.ObjectBucket, cm *corev1.ConfigMap, sec *corev1.Secret, errs []error) {

	var err error
	// The cap(errs) must be large enough to encapsulate errors returned by all 3 *ForClaimKey funcs
	errs = make([]error, 0, 3)
	groupErrors := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	ob, err = c.objectBucketForClaimKey(key)
	groupErrors(err)
	cm, err = configMapForClaimKey(key, c.clientset)
	groupErrors(err)
	sec, err = secretForClaimKey(key, c.clientset)
	groupErrors(err)

	return
}

// Deleting the resources generated by a Provision or Grant call is triggered by the delete of
// the OBC. However, a finalizer is added to the OBC so that we can cleanup up the other resources
// created by a Provision or Grant call. Since the secret and configmap's ownerReference is the OBC
// they will be garbage collected once their finalizers are removed. The OB must be explicitly
// deleted since it is a global resource and cannot have a namespaced ownerReference. The last step
// is to remove the finalizer on the OBC so it too will be garbage collected.
// Returns err if we can't delete one or more of the resources, the final returned error being
// somewhat arbitrary.
func (c *obcController) deleteResources(ob *v1alpha1.ObjectBucket, cm *corev1.ConfigMap, s *corev1.Secret, obc *v1alpha1.ObjectBucketClaim) (err error) {

	if delErr := deleteObjectBucket(ob, c.libClientset); delErr != nil {
		log.Error(delErr, "error deleting objectBucket", ob.Name)
		err = delErr
	}
	if delErr := releaseSecret(s, c.clientset); delErr != nil {
		log.Error(delErr, "error releasing secret")
		err = delErr
	}
	if delErr := releaseConfigMap(cm, c.clientset); delErr != nil {
		log.Error(delErr, "error releasing configMap")
		err = delErr
	}
	if delErr := releaseOBC(obc, c.libClientset); delErr != nil {
		log.Error(delErr, "error releasing obc")
		err = delErr
	}
	return err
}

// Add finalizer and labels to the OBC.
func (c *obcController) setOBCMetaFields(obc *v1alpha1.ObjectBucketClaim) (err error) {
	clib := c.libClientset

	log.Info("getting OBC to set metadata fields")
	obc, err = clib.ObjectbucketV1alpha1().ObjectBucketClaims(obc.Namespace).Get(obc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting obc: %v", err)
	}

	obc.SetFinalizers([]string{finalizer})
	obc.SetLabels(c.provisionerLabels)

	log.Info("updating OBC metadata")
	obc, err = updateClaim(clib, obc, defaultRetryBaseInterval, defaultRetryTimeout)
	if err != nil {
		return fmt.Errorf("error configuring obc metadata: %v", err)
	}

	return nil
}

func (c *obcController) objectBucketForClaimKey(key string) (*v1alpha1.ObjectBucket, error) {
	log.Info("getting objectBucket for key", "key", key)
	name, err := objectBucketNameFromClaimKey(key)
	if err != nil {
		return nil, err
	}
	ob, err := c.libClientset.ObjectbucketV1alpha1().ObjectBuckets().Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ob, nil
}
