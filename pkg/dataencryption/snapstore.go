package dataencryption

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/minio/sio"
	"golang.org/x/crypto/hkdf"

	"github.com/gardener/etcd-backup-restore/pkg/types"
)

type decoratedSnapStore struct {
	snapstore            types.SnapStore
	encryptionConfigPath string
}

func (r *decoratedSnapStore) readEncryptionMasterKey() string {
	if r.encryptionConfigPath == "" {
		return ""
	}
	content, err := os.ReadFile(r.encryptionConfigPath)
	if err != nil {
		panic(fmt.Errorf("readEncryptionMasterKey: failed to read %q as %v", r.encryptionConfigPath, err))
	}

	cfg := types.SnapstoreEncryptionConfig{}
	err = json.Unmarshal(content, &cfg)
	if err != nil {
		panic(fmt.Errorf("readEncryptionMasterKey: failed to unmarshal %q as %v", r.encryptionConfigPath, err))
	}

	return cfg.Key
}

func (r *decoratedSnapStore) deriveEncryptionKey(masterKey string, snapshot types.Snapshot) ([32]byte, error) {
	var key [32]byte
	nonce := snapshot.SnapName // use snapName as the nonce
	kdf := hkdf.New(sha256.New, []byte(masterKey), []byte(nonce), nil)
	if _, err := io.ReadFull(kdf, key[:]); err != nil {
		return [32]byte{}, err
	}
	return key, nil
}

func (r *decoratedSnapStore) Fetch(snapshot types.Snapshot) (io.ReadCloser, error) {
	masterKey := r.readEncryptionMasterKey()
	if len(masterKey) == 0 {
		return r.snapstore.Fetch(snapshot)
	}

	key, err := r.deriveEncryptionKey(masterKey, snapshot)
	if err != nil {
		return nil, fmt.Errorf("deriveEncryptionKey failed as %v", err)
	}

	originalEncryptedDataReader, err := r.snapstore.Fetch(snapshot)
	decryptedDataReader, err := sio.DecryptReader(originalEncryptedDataReader, sio.Config{
		Key:          key[:],
		CipherSuites: []byte{sio.AES_256_GCM},
	})
	if err != nil {
		return nil, fmt.Errorf("sio.DecryptReader failed as %v", err)
	}

	return &readerCloser{r: decryptedDataReader, c: originalEncryptedDataReader}, err
}

func (r *decoratedSnapStore) List() (types.SnapList, error) {
	return r.snapstore.List()
}

func (r *decoratedSnapStore) Save(snapshot types.Snapshot, originalUnencryptedDataReader io.ReadCloser) error {
	masterKey := r.readEncryptionMasterKey()
	if len(masterKey) == 0 {
		return r.snapstore.Save(snapshot, originalUnencryptedDataReader)
	}

	key, err := r.deriveEncryptionKey(masterKey, snapshot)
	if err != nil {
		return fmt.Errorf("deriveEncryptionKey failed as %v", err)
	}

	encryptedDataReader, err := sio.EncryptReader(originalUnencryptedDataReader, sio.Config{
		Key:          key[:],
		CipherSuites: []byte{sio.AES_256_GCM},
	})
	if err != nil {
		return fmt.Errorf("sio.EncryptReader failed as %v", err)
	}

	return r.snapstore.Save(snapshot, &readerCloser{r: encryptedDataReader, c: originalUnencryptedDataReader})
}

func (r *decoratedSnapStore) Delete(snapshot types.Snapshot) error {
	return r.snapstore.Delete(snapshot)
}

// DecorateSnapStore returns a decorated SnapStore that will apply encryption and decryption automatically if configured to do so
func DecorateSnapStore(snapstore types.SnapStore, encryptionConfigPath string) types.SnapStore {
	return &decoratedSnapStore{snapstore: snapstore, encryptionConfigPath: encryptionConfigPath}
}
