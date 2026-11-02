package etcd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

type namespaceWALFile struct {
	name string
	info os.FileInfo
}

type namespaceWALInventory struct {
	directory os.FileInfo
	files     []namespaceWALFile
	segments  []string
}

// Runtime recovery recognizes only etcd's two native preallocation names.
// Offline observation/preparation still uses the stricter baselineWALNames.
func inspectNamespaceWAL(dir string) (namespaceWALInventory, error) {
	var inventory namespaceWALInventory
	if err := baselineRegularDirectory(dir); err != nil {
		return inventory, err
	}
	var err error
	inventory.directory, err = os.Lstat(dir)
	if err != nil {
		return inventory, errors.WithStack(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return inventory, errors.WithStack(err)
	}
	for _, entry := range entries {
		if err := inventory.add(dir, entry.Name()); err != nil {
			return inventory, err
		}
	}
	if len(inventory.segments) == 0 {
		return inventory, errors.Wrap(ErrOfflineBaseline, "existing runtime WAL required; no bootstrap")
	}
	return inventory, nil
}

func (i *namespaceWALInventory) add(dir, name string) error {
	path := filepath.Join(dir, name)
	if err := baselineRegularFile(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if name == "0.tmp" || name == "1.tmp" {
		// wal.Open's allocator may reuse this inode. Reject links anywhere,
		// not just aliases to segments visible in this directory.
		if err := namespacePreallocationUnlinked(info); err != nil {
			return err
		}
	} else if err := i.addSegment(name); err != nil {
		return err
	}
	i.files = append(i.files, namespaceWALFile{name: name, info: info})
	return nil
}

func (i *namespaceWALInventory) addSegment(name string) error {
	var sequence, index uint64
	if _, err := fmt.Sscanf(name, "%016x-%016x.wal", &sequence, &index); err != nil || name != fmt.Sprintf("%016x-%016x.wal", sequence, index) {
		return errors.Wrap(ErrOfflineBaseline, "unknown runtime WAL directory entry")
	}
	if len(i.segments) != 0 {
		var previous uint64
		if _, err := fmt.Sscanf(i.segments[len(i.segments)-1], "%016x-", &previous); err != nil || sequence != previous+1 {
			return errors.Wrap(ErrOfflineBaseline, "runtime WAL segment gap")
		}
	}
	i.segments = append(i.segments, name)
	return nil
}

func readRuntimeNamespaceWAL(dir string) (*baselineWALImage, error) {
	inventory, err := inspectNamespaceWAL(dir)
	if err != nil {
		return nil, err
	}
	image := newBaselineWALImage()
	image.keepPayloads = true
	image.genesisAllowed = inventory.segments[0] == "0000000000000000-0000000000000000.wal"
	var crc uint32
	for _, name := range inventory.segments {
		path := filepath.Join(dir, name)
		image.segmentPaths = append(image.segmentPaths, path)
		crc, err = readBaselineSegment(path, image, crc)
		if err != nil {
			return nil, errors.Wrapf(err, "read runtime WAL %s", name)
		}
	}
	if !image.metadataSeen {
		return nil, errors.Wrap(ErrOfflineBaseline, "missing runtime WAL metadata record")
	}
	// This is selected namespace replay input, NOT an offline file fingerprint.
	// No preallocation bytes are interpreted as records or baseline equality.
	return image, nil
}
