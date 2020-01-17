package objectbucketclaim

import (
	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/apis/objectbucket.io/v1alpha1"
	"github.com/kube-object-storage/lib-bucket-provisioner/pkg/provisioner/api"
)

type dummyHandler struct {}

func (d dummyHandler) Provision(options *api.BucketOptions) (*v1alpha1.ObjectBucket, error) {
	log.Info("PROVISION!")
	return &v1alpha1.ObjectBucket{}, nil
}

func (d dummyHandler) Grant(options *api.BucketOptions) (*v1alpha1.ObjectBucket, error) {
	log.Info("GRANT!")
	return &v1alpha1.ObjectBucket{}, nil
}

func (d dummyHandler) Delete(ob *v1alpha1.ObjectBucket) error {
	log.Info("DELETE!")
	return nil
}

func (d dummyHandler) Revoke(ob *v1alpha1.ObjectBucket) error {
	log.Info("REVOKE!")
	return nil

}
