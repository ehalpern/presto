package database

import (
	"flag"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/aws"
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/spec"
)

var (
	indexCacheSize = uint64(1 << 30) // 1GB
	maxOpenFiles = 1024
	tableCacheSize = uint64(128)
	tableCacheDir = "/tmp/presto-noms-cache"
)

var (
	storeCache = struct {
		cache map[string]chunks.ChunkStore
		lock sync.Mutex
	} {
		cache: make(map[string]chunks.ChunkStore),
	}
)

func RegisterDatabaseFlags(fs *flag.FlagSet) {
	fs.Uint64Var(&indexCacheSize,"db-index-cache-size", indexCacheSize, "nbs index block cache size (bytes)")
	fs.IntVar(&maxOpenFiles,"db-max-open-files", maxOpenFiles, "(when using file-system storage) maximum number of open files to allow")
}

// Preload forces load of database |dbName| and caching of chunk store. Call this function to
// trigger the initial db load before the first query to hide initial index loading time.
func Preload(dbName string) error {
	if _, err := spec.ForDatabase(dbName); err != nil {
		return err
	}
	log.Printf("Preloading %s...", dbName)
	// No need to block here. Subsequent calls to GetDatabase will block until
	// initial loading is complete
	go func() {
		db, err := GetDatabase(dbName)
		if err != nil {
			log.Printf("Warning: Failed to preload %s: %s\n", dbName, err)
			return
		}
		db.Rebase()
		log.Printf("Preload %s complete", dbName)
	}()
	return nil
}

// GetDatabase retrieves a database connection. Share the chunk store across all connections
// to the same database.
// TODO: It would be preferable to use a new ChunkStore for each request so that
// database Stats are isolated for each request. This is currently not practical
// because chunks.Factory.CreateStore... is too costly on a large database.
// (aws://bucketdb-manifests:bucketdb-tables/p/taxidata for example takes 4-8 seconds
// running from an aws instance).
func GetDatabase(dbName string) (db datas.Database, err error) {
	start := time.Now()
	defer func() {
		log.Printf("GetDatabase(%s): %v", dbName, time.Since(start))
	}()
	protocol, prefix, name, err := dbNameParts(dbName)
	if err != nil {
		return db, err
	}
	cs := func() chunks.ChunkStore {
		sc := storeCache
		sc.lock.Lock()
		defer sc.lock.Unlock()
		cs, ok := storeCache.cache[prefix]
		if !ok {
			f := chunksFactory(protocol, prefix)
			cs = f.CreateStore(name)
			sc.cache[prefix] = cs
		}
		return cs
	}()
	d.PanicIfFalse(cs != nil)
	return datas.NewDatabase(cs), nil
}

// Returns protocol appropriate factory
func chunksFactory(protocol, prefix string) chunks.Factory {
	start := time.Now()
	defer func() {
		log.Printf("chunksFactory(%s): %v", prefix, time.Since(start))
	}()
	switch protocol {
	case "nbs":
		return nbs.NewLocalStoreFactory(prefix, indexCacheSize, maxOpenFiles)
	case "aws":
		parts := strings.Split(prefix, ":")
		d.PanicIfFalse(len(parts) == 2)
		manifest, bucket := parts[0], parts[1]
		// TODO: Setting tableCacheSize = 0 and tableCacheDir = "" causes panic, but should not
		// be required for a read-only db.
		return nbs.NewAWSStoreFactory(aws.NewSession(), manifest, bucket, maxOpenFiles, indexCacheSize, tableCacheSize, tableCacheDir)
	default:
		panic(fmt.Errorf("unsupported protocol %s", protocol))
	}
}

// Returns the |protocol|, db |prefix| and |namespace| of the database.
// If the protocol is nbs, the prefix is the directory and the namespace is the base name
// If the protocol is aws, the prefix is the manifest:bucket spec and the namespace is
// the following path.
func dbNameParts(dbName string) (protocol, prefix, namespace string, err error) {
	sp, err := spec.ForDatabase(dbName)
	if err != nil {
		return "", "", "", err
	}
	protocol = sp.Protocol
	switch protocol {
	case "nbs":
		rest := sp.DatabaseName
		prefix, namespace = path.Dir(rest), path.Base(rest)
		if prefix == "" || namespace == "" {
			return "", "", "", fmt.Errorf("db spec prefix or namespace", dbName)
		}
		return protocol, prefix, namespace, nil
	case "aws":
		rest := strings.TrimPrefix(sp.Href(), sp.Protocol+"://")
		split := strings.SplitN(rest, "/", 2)
		if len(split) != 2 {
			return "", "", "", fmt.Errorf("db spec missing namespace", dbName)
		}
		return protocol, split[0], "/" + split[1], nil
	}
	panic(fmt.Errorf("unsupported protocol %s", sp.Protocol))
}
