// Package tlsutil generates Kairo's TLS material (a private CA and server
// certificates it signs) and builds the client and server tls.Configs.
//
// Certificates carry the extensions strict verifiers require (Python 3.13's
// VERIFY_X509_STRICT, rustls/webpki): explicit SubjectKeyId on both
// certificates, AuthorityKeyId on the leaf, critical key usage, server-auth
// extended key usage and subjectAltName entries for every host.
package tlsutil

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kairo/internal/secfile"
)

// File names written into the TLS directory.
const (
	CACertFile     = "ca.pem"
	CAKeyFile      = "ca-key.pem"
	ServerCertFile = "server.pem"
	ServerKeyFile  = "server-key.pem"
)

const (
	caValidity     = 10 * 365 * 24 * time.Hour
	serverValidity = 2 * 365 * 24 * time.Hour
	backdate       = time.Hour
)

// InitCA creates dir if needed and writes a new ECDSA P-256 CA certificate
// (ca.pem) and its PKCS#8 private key (ca-key.pem, mode 0600). It refuses to
// run when either file already exists.
func InitCA(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	certPath := filepath.Join(dir, CACertFile)
	keyPath := filepath.Join(dir, CAKeyFile)
	for _, path := range []string{certPath, keyPath} {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists; refusing to overwrite the CA", path)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ski, err := subjectKeyID(&key.PublicKey)
	if err != nil {
		return err
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Kairo CA"},
		NotBefore:             now.Add(-backdate),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
		SubjectKeyId:          ski,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyPEM, err := encodeKey(key)
	if err != nil {
		return err
	}
	if err := writeExclusive(keyPath, keyPEM, 0o600); err != nil {
		return err
	}
	if err := writeExclusive(certPath, encodeCert(der), 0o644); err != nil {
		_ = os.Remove(keyPath)
		return err
	}
	return nil
}

// IssueServer signs a new ECDSA P-256 server certificate with the CA in dir
// and writes server.pem and server-key.pem (mode 0600), replacing existing
// server files. Each host is an IP address or a DNS name; at least one is
// required and the first becomes the subject common name.
func IssueServer(dir string, hosts []string) error {
	var ips []net.IP
	var names []string
	var cleaned []string
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		cleaned = append(cleaned, host)
		if ip := net.ParseIP(host); ip != nil {
			ips = append(ips, ip)
		} else {
			names = append(names, host)
		}
	}
	if len(cleaned) == 0 {
		return errors.New("at least one host is required")
	}
	caCert, caKey, err := loadCA(dir)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ski, err := subjectKeyID(&key.PublicKey)
	if err != nil {
		return err
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	notAfter := now.Add(serverValidity)
	if notAfter.After(caCert.NotAfter) {
		notAfter = caCert.NotAfter
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cleaned[0]},
		NotBefore:             now.Add(-backdate),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		SubjectKeyId:          ski,
		AuthorityKeyId:        caCert.SubjectKeyId,
		DNSNames:              names,
		IPAddresses:           ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyPEM, err := encodeKey(key)
	if err != nil {
		return err
	}
	if err := writeReplace(filepath.Join(dir, ServerKeyFile), keyPEM, 0o600); err != nil {
		return err
	}
	return writeReplace(filepath.Join(dir, ServerCertFile), encodeCert(der), 0o644)
}

// LoadCertPool returns a pool holding the PEM certificates in caPEM.
func LoadCertPool(caPEM []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("no CA certificate found in PEM data")
	}
	return pool, nil
}

// ClientConfig returns a TLS 1.3 client config trusting the certificates in
// caPEM, or the system roots when caPEM is empty.
func ClientConfig(caPEM []byte) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	if len(bytes.TrimSpace(caPEM)) == 0 {
		return cfg, nil
	}
	pool, err := LoadCertPool(caPEM)
	if err != nil {
		return nil, err
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// ServerConfig returns the TLS 1.3 server config; the caller adds certificates.
func ServerConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13}
}

// CAEnv encodes every certificate in caPEM (several during a CA rotation) as
// single-line base64 of their concatenated DER, safe to pass in an
// environment variable.
func CAEnv(caPEM []byte) (string, error) {
	var der []byte
	rest := caPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return "", fmt.Errorf("parse CA certificate: %w", err)
		}
		der = append(der, block.Bytes...)
	}
	if len(der) == 0 {
		return "", errors.New("no CA certificate found in PEM data")
	}
	return base64.StdEncoding.EncodeToString(der), nil
}

// CAFromEnv decodes a CAEnv value back into PEM.
func CAFromEnv(value string) ([]byte, error) {
	der, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("decode CA: %w", err)
	}
	certs, err := x509.ParseCertificates(der)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	if len(certs) == 0 {
		return nil, errors.New("no CA certificate in value")
	}
	var out []byte
	for _, cert := range certs {
		out = append(out, encodeCert(cert.Raw)...)
	}
	return out, nil
}

// VerifyServer checks that cert chains to a certificate of caPEM and covers
// host, the address clients and attempts reach the daemon at.
func VerifyServer(cert tls.Certificate, caPEM []byte, host string) error {
	pool, err := LoadCertPool(caPEM)
	if err != nil {
		return err
	}
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return errors.New("no server certificate")
		}
		if leaf, err = x509.ParseCertificate(cert.Certificate[0]); err != nil {
			return err
		}
	}
	intermediates := x509.NewCertPool()
	for _, der := range cert.Certificate[1:] {
		if c, err := x509.ParseCertificate(der); err == nil {
			intermediates.AddCert(c)
		}
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: intermediates, DNSName: host, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	return err
}

func loadCA(dir string) (*x509.Certificate, crypto.Signer, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, CACertFile))
	if err != nil {
		return nil, nil, fmt.Errorf("read CA certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, CAKeyFile))
	if err != nil {
		return nil, nil, fmt.Errorf("read CA key: %w", err)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, nil, errors.New("CA certificate file holds no CERTIFICATE block")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	if !cert.IsCA {
		return nil, nil, errors.New("CA certificate is not a CA")
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil || keyBlock.Type != "PRIVATE KEY" {
		return nil, nil, errors.New("CA key file holds no PRIVATE KEY block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA key: %w", err)
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, nil, errors.New("CA key cannot sign")
	}
	type equaler interface{ Equal(crypto.PublicKey) bool }
	if pub, ok := signer.Public().(equaler); !ok || !pub.Equal(cert.PublicKey) {
		return nil, nil, errors.New("CA key does not match CA certificate")
	}
	return cert, signer, nil
}

func subjectKeyID(pub *ecdsa.PublicKey) ([]byte, error) {
	ecdhPub, err := pub.ECDH()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum(ecdhPub.Bytes())
	return sum[:], nil
}

func randomSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	if serial.Sign() == 0 {
		serial.SetInt64(1)
	}
	return serial, nil
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKey(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// writeExclusive creates path, failing if it already exists.
func writeExclusive(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return restrictSecret(path, mode)
}

// restrictSecret makes an owner-only file owner-only on Windows too, where
// the mode is ignored and the directory's entries are inherited.
func restrictSecret(path string, mode os.FileMode) error {
	if mode&0o077 != 0 {
		return nil
	}
	if err := secfile.Restrict(path); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("restrict %s to its owner: %w", path, err)
	}
	return nil
}

// writeReplace writes data to a temporary file beside path and renames it
// into place, so the final file has mode regardless of any previous file.
func writeReplace(path string, data []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := f.Chmod(mode); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if mode&0o077 == 0 {
		if err := secfile.Restrict(tmp); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("restrict %s to its owner: %w", path, err)
		}
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
