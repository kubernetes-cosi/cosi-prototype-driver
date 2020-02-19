package pluginregistration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/yard-turkey/cosi-prototype-driver/pkg/apis/objectbucket/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"math/big"
	"net"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type certificateRegistrar struct {
	client       client.Client
	scheme       *runtime.Scheme
	ctx          context.Context
	registration *v1alpha1.PluginRegistration
}

func newCertificateRegistrar(c client.Client, scheme *runtime.Scheme, instance *v1alpha1.PluginRegistration) *certificateRegistrar {
	return &certificateRegistrar{client: c, scheme: scheme, ctx: context.Background(), registration: instance}
}

const bits = 2048

func (cr *certificateRegistrar) issue() error {
	ref, err := cr.generateCertificateSecret()
	err = cr.registerSecret(ref)
	if err != nil {
		return err
	}
	return nil
}

func (cr *certificateRegistrar) generateCertificateSecret() (*corev1.SecretReference, error) {
	cert, key, err := generateNewSerializedCredentials(cr.registration.Spec.Host)
	if err != nil {
		return nil, err
	}
	return cr.createNewCertSecret(cert, key)
}

func generateNewSerializedCredentials(host string) ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, err
	}

	template, err := newCertificateTemplate(host)
	if err != nil {
		return nil, nil, err
	}

	serializedCert, err := newSerializedCert(template, privateKey)
	if err != nil {
		return nil, nil, err
	}

	serializedKey := newSerializedKey(privateKey)

	return serializedCert, serializedKey, nil
}

func (cr *certificateRegistrar) createNewCertSecret(cert, key []byte) (*corev1.SecretReference, error) {
	sec := cr.newTLSSecret(cr.registration.Name, cr.registration.Namespace, cert, key)
	err := cr.client.Create(cr.ctx, sec)
	if err != nil {
		return nil, err
	}
	return &corev1.SecretReference{Namespace: sec.Namespace, Name: sec.Name}, nil
}

func (cr *certificateRegistrar) registerSecret(secretReference *corev1.SecretReference) error {
	cr.registration.Status.CertSecret = secretReference
	return cr.client.Status().Update(cr.ctx, cr.registration)
}

const (
	org        = "cosi.io"
	commonName = "PluginCertificate"
	validFor   = 365 // days
)

func newCertificateTemplate(host string) (*x509.Certificate, error) {
	snMax := new(big.Int).Lsh(big.NewInt(1), 128)
	snMax, err := rand.Int(rand.Reader, snMax)
	if err != nil {
		return nil, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validFor)

	tmp := &x509.Certificate{
		SerialNumber: snMax,
		Subject: pkix.Name{
			Organization: []string{org},
			CommonName:   commonName,
		},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		IsCA:        false,
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	if ip := net.ParseIP(host); ip != nil {
		tmp.IPAddresses = append(tmp.IPAddresses, ip)
	} else {
		tmp.DNSNames = append(tmp.DNSNames, host)
	}

	return tmp, nil
}

func newSerializedCert(template *x509.Certificate, privateKey *rsa.PrivateKey) ([]byte, error) {
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, privateKey.Public(), privateKey)
	if err != nil {
		return nil, err
	}
	pemCertBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})
	return pemCertBytes, nil
}

func newSerializedKey(privateKey *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
}

func (cr *certificateRegistrar) newTLSSecret(name, ns string, cert, key []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Type: "kubernetes.io/tls",
		Data: map[string][]byte{
			"tls.crt": cert,
			"tls.key": key,
		},
	}
}

func (cr *certificateRegistrar) revoke() error {
	sec := &corev1.Secret{}
	err := cr.client.Get(cr.ctx, objectKeyFromSecretRef(cr.registration.Status.CertSecret), sec)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil // The secret was deleted at some point prior to this call.  Other errors may be intermittent.
		}
		return err
	}
	err = cr.client.Delete(cr.ctx, sec)
	return err
}

func objectKeyFromSecretRef(sr *corev1.SecretReference) client.ObjectKey {
	return client.ObjectKey{
		Namespace: sr.Namespace,
		Name:      sr.Name,
	}
}
