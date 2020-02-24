package tls_util

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)



// GenerateSelfSignedRootCert creates a self signed x509 certificate that should only be used by the controller to verify
// plugin responses.  Plugin certificates should be generated as leafs from this root certificate.
func GenerateSelfSignedRootCert() (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := newPrivateKey()
	if err != nil {
		return nil, nil, err
	}

	cert, err := generateSelfSignedCert(privateKey)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}

func generateSelfSignedCert(key *rsa.PrivateKey) (*x509.Certificate, error) {
	rootTemplate, err := newCertificateTemplate([]string{"127.0.0.1"})
	if err != nil {
		return nil, err
	}
	return generateCert(rootTemplate, rootTemplate, key.PublicKey, key)
}

func GeneratePluginServerChildCert(hosts []string, parentCertificate *x509.Certificate) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := newPrivateKey()
	if err != nil {
		return nil, nil, err
	}
	template, err := newCertificateTemplate(hosts)
	if err != nil {
		return nil, nil, err
	}
	cert, err := generateCert(template, parentCertificate, privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}
	return cert, privateKey, nil
}

func newCertificateTemplate(hosts []string) (*x509.Certificate, error) {

	sn, err := newSerialNumber()
	if err != nil {
		return nil, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(validFor)

	tmp := &x509.Certificate{
		SerialNumber: sn,
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

	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			tmp.IPAddresses = append(tmp.IPAddresses, ip)
		} else {
			tmp.DNSNames = append(tmp.DNSNames, host)
		}
	}

	return tmp, nil
}

func newPrivateKey() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, bits)
}

func generateCert(template, parent *x509.Certificate, publicKey rsa.PublicKey, privateKey *rsa.PrivateKey) (*x509.Certificate, error) {
	certBytes, err := x509.CreateCertificate(rand.Reader, template, parent, publicKey, privateKey)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certBytes)
}

const (
	org        = "cosi.io"
	commonName = "PluginCertificate"
	validFor   = 365 // days
)

const bits = 2048



func newSerialNumber() (*big.Int, error) {
	snMax := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, snMax)
}

func PEMSerializedCertificate(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:    "CERTIFICATE",
		Headers: nil,
		Bytes:   c.Raw,
	})
}

func PemSerializedRSAKey(r *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:    "RSA PRIVATE KEY",
		Bytes:   x509.MarshalPKCS1PrivateKey(r),
	})
}