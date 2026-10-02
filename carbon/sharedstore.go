package carbon

import (
	"errors"
	"path/filepath"
	"reflect"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-whisper/store"
)

func sharedStorePath(cfg *Config) string {
	if cfg.Whisper.StoreDir != "" {
		return cfg.Whisper.StoreDir
	}
	return filepath.Join(cfg.Whisper.DataDir, ".store")
}

func validateStorageConfig(cfg *Config) error {
	if cfg.Whisper.StorageBackend == "" {
		cfg.Whisper.StorageBackend = "files"
	}
	if cfg.Whisper.StorageBackend != "files" && cfg.Whisper.StorageBackend != "pebble" {
		return errors.New("whisper.storage-backend must be files or pebble")
	}
	if cfg.Buckyd.Enabled && cfg.Whisper.StorageBackend != "pebble" {
		return errors.New("embedded buckyd requires whisper.storage-backend = pebble")
	}
	if cfg.Whisper.StorageBackend != "pebble" {
		return nil
	}
	if cfg.Whisper.StoreCacheSize <= 0 || cfg.Whisper.StoreMemTableSize < 64<<10 {
		return errors.New("shared storage requires positive cache size and memtable size >= 65536")
	}
	if cfg.Whisper.OnlineMigration {
		return errors.New("online-migration is not supported by shared storage")
	}
	for _, schema := range cfg.Whisper.Schemas {
		if schema.Migration != nil && *schema.Migration {
			return errors.New("schema migration is not supported by shared storage")
		}
	}
	for _, quota := range cfg.Whisper.Quotas {
		if quota.PhysicalSize > 0 {
			return errors.New("namespace physical-size quotas are unavailable with shared storage; use logical-size quotas")
		}
	}
	return nil
}

func storageSettingsChanged(old, next *Config) bool {
	if old.Whisper.StorageBackend != next.Whisper.StorageBackend || !reflect.DeepEqual(old.Buckyd, next.Buckyd) {
		return true
	}
	if next.Whisper.StorageBackend != "pebble" {
		// The store path derives from data-dir, which file backends may still hot-reload.
		return false
	}
	return sharedStorePath(old) != sharedStorePath(next) ||
		old.Whisper.StoreCacheSize != next.Whisper.StoreCacheSize ||
		old.Whisper.StoreMemTableSize != next.Whisper.StoreMemTableSize
}

type storeStats struct{ db *store.Store }

func (s storeStats) Stat(send helper.StatCallback) {
	stats := s.db.Stats()
	send("diskBytes", float64(stats.DiskBytes))
	send("walBytes", float64(stats.WALBytes))
	send("memTableBytes", float64(stats.MemTableBytes))
}
