package renter

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/siafile"
	"gitlab.com/NebulousLabs/Sia/persist"
	"gitlab.com/NebulousLabs/Sia/types"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/writeaheadlog"
)

const (
	logFile = modules.RenterDir + ".log"
	// PersistFilename is the filename to be used when persisting renter information to a JSON file
	PersistFilename = "renter.json"
	// ShareExtension is the extension to be used
	ShareExtension = ".sia"
	// SiaDirMetadata is the name of the metadata file for the sia directory
	SiaDirMetadata = ".siadir"
	// walFile is the filename of the renter's writeaheadlog's file.
	walFile = modules.RenterDir + ".wal"
)

var (
	//ErrBadFile is an error when a file does not qualify as .sia file
	ErrBadFile = errors.New("not a .sia file")
	// ErrIncompatible is an error when file is not compatible with current version
	ErrIncompatible = errors.New("file is not compatible with current version")
	// ErrNoNicknames is an error when no nickname is given
	ErrNoNicknames = errors.New("at least one nickname must be supplied")
	// ErrNonShareSuffix is an error when the suffix of a file does not match the defined share extension
	ErrNonShareSuffix = errors.New("suffix of file must be " + ShareExtension)

	dirMetadataHeader = persist.Metadata{
		Header:  "Sia Directory Metadata",
		Version: persistVersion,
	}
	settingsMetadata = persist.Metadata{
		Header:  "Renter Persistence",
		Version: persistVersion,
	}

	shareHeader  = [15]byte{'S', 'i', 'a', ' ', 'S', 'h', 'a', 'r', 'e', 'd', ' ', 'F', 'i', 'l', 'e'}
	shareVersion = "0.4"

	// Persist Version Numbers
	persistVersion040 = "0.4"
	persistVersion133 = "1.3.3"
)

type (
	// dirMetadata contains the metadata information about a renter directory
	dirMetadata struct {
		MinHealth        int
		RecentRepairTime int64
		RecentUpdateTime int64
	}

	// persist contains all of the persistent renter data.
	persistence struct {
		MaxDownloadSpeed int64
		MaxUploadSpeed   int64
		StreamCacheSize  uint64
	}
)

// MarshalSia implements the encoding.SiaMarshaller interface, writing the
// file data to w.
func (f *file) MarshalSia(w io.Writer) error {
	enc := encoding.NewEncoder(w)

	// encode easy fields
	err := enc.EncodeAll(
		f.name,
		f.size,
		f.masterKey,
		f.pieceSize,
		f.mode,
	)
	if err != nil {
		return err
	}
	// COMPATv0.4.3 - encode the bytesUploaded and chunksUploaded fields
	// TODO: the resulting .sia file may confuse old clients.
	err = enc.EncodeAll(f.pieceSize*f.numChunks()*uint64(f.erasureCode.NumPieces()), f.numChunks())
	if err != nil {
		return err
	}

	// encode erasureCode
	switch code := f.erasureCode.(type) {
	case *siafile.RSCode:
		err = enc.EncodeAll(
			"Reed-Solomon",
			uint64(code.MinPieces()),
			uint64(code.NumPieces()-code.MinPieces()),
		)
		if err != nil {
			return err
		}
	default:
		if build.DEBUG {
			panic("unknown erasure code")
		}
		return errors.New("unknown erasure code")
	}
	// encode contracts
	if err := enc.Encode(uint64(len(f.contracts))); err != nil {
		return err
	}
	for _, c := range f.contracts {
		if err := enc.Encode(c); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalSia implements the encoding.SiaUnmarshaller interface,
// reconstructing a file from the encoded bytes read from r.
func (f *file) UnmarshalSia(r io.Reader) error {
	dec := encoding.NewDecoder(r)

	// COMPATv0.4.3 - decode bytesUploaded and chunksUploaded into dummy vars.
	var bytesUploaded, chunksUploaded uint64

	// Decode easy fields.
	err := dec.DecodeAll(
		&f.name,
		&f.size,
		&f.masterKey,
		&f.pieceSize,
		&f.mode,
		&bytesUploaded,
		&chunksUploaded,
	)
	if err != nil {
		return err
	}
	f.staticUID = persist.RandomSuffix()

	// Decode erasure coder.
	var codeType string
	if err := dec.Decode(&codeType); err != nil {
		return err
	}
	switch codeType {
	case "Reed-Solomon":
		var nData, nParity uint64
		err = dec.DecodeAll(
			&nData,
			&nParity,
		)
		if err != nil {
			return err
		}
		rsc, err := siafile.NewRSCode(int(nData), int(nParity))
		if err != nil {
			return err
		}
		f.erasureCode = rsc
	default:
		return errors.New("unrecognized erasure code type: " + codeType)
	}

	// Decode contracts.
	var nContracts uint64
	if err := dec.Decode(&nContracts); err != nil {
		return err
	}
	f.contracts = make(map[types.FileContractID]fileContract)
	var contract fileContract
	for i := uint64(0); i < nContracts; i++ {
		if err := dec.Decode(&contract); err != nil {
			return err
		}
		f.contracts[contract.ID] = contract
	}
	return nil
}

// createDir creates directory in the renter directory
func (r *Renter) createDir(siapath string) error {
	// Enforce nickname rules.
	if err := validateSiapath(siapath); err != nil {
		return err
	}

	// Create direcotry
	path := filepath.Join(r.filesDir, siapath)
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}

	// Make sure all parent directories have metadata files
	for path != filepath.Dir(r.filesDir) {
		if err := r.createDirMetadata(path); err != nil {
			return err
		}
		path = filepath.Dir(path)
	}
	return nil
}

// createDirMetadata makes sure there is a metadata file in the directory and
// creates one as needed
func (r *Renter) createDirMetadata(path string) error {
	fullPath := filepath.Join(path, SiaDirMetadata)
	// Check if metadata file exists
	if _, err := os.Stat(fullPath); err == nil {
		return nil
	}

	// Initialize metadata, set MinHealth to MaxInt64 so empty directories
	// won't be viewed as being the most in need
	data := dirMetadata{
		MinHealth:        math.MaxInt64,
		RecentRepairTime: int64(0),
		RecentUpdateTime: time.Now().UnixNano(),
	}
	return r.saveDirMetadata(path, data)
}

// findMinDirRedundancy walks the renter's file directory and finds the
// directory with the lowest minHealth. Since it uses Walk() the directory
// returned will be the lowest level directory
func (r *Renter) findMinDirRedundancy() (string, error) {
	var metadata, metadataAnyTime dirMetadata
	dir := r.filesDir
	minHealth := math.MaxInt64
	dirAnyTime := r.filesDir // dirAnyTime is the dir regardless of timeBetweenRepair

	// Read renter files directory metadata
	persistDirMetadata, err := r.loadDirMetadata(dir)
	if err != nil {
		r.log.Printf("WARN: Could not load directory metadata for %v: %v", dir, err)
		return dir, err
	}
	// This Walk will log errors but not return them
	_ = filepath.Walk(r.filesDir, func(path string, info os.FileInfo, err error) error {
		// Skip files
		if !info.IsDir() {
			return nil
		}

		// Skip empty directories, directories when they are created should have
		// the sia folder metadata and temp metadata files
		fileinfos, err := ioutil.ReadDir(path)
		if len(fileinfos) <= 2 {
			// Confirm that the two files are the metadata files, log error if
			// that isn't the case
			if len(fileinfos) < 2 {
				r.log.Printf("WARN: directory %v doesn't appear to have folder metadata files\n", path)
			}
			return nil
		}

		// Load directory metadata
		md, err := r.loadDirMetadata(path)
		if err != nil {
			r.log.Printf("WARN: Could not load directory metadata for %v: %v\n", path, err)
			return nil
		}

		// Check minHealth and record lower health
		if md.MinHealth > minHealth {
			return nil
		}
		metadataAnyTime = md
		dirAnyTime = path
		// Check if directory was recently repaired, if so skip
		if md.RecentRepairTime < time.Now().UnixNano()-timeBetweenRepair {
			metadata = md
			minHealth = md.MinHealth
			dir = path
		}
		return nil
	})

	// Check to see if directory returned is the top level files directory. If
	// so and it was recently repaired, submit the lowest heatlh directory
	// regardless of its RecentRepairTime
	if dir == r.filesDir && persistDirMetadata.RecentRepairTime >= time.Now().UnixNano()-timeBetweenRepair {
		metadataAnyTime.RecentRepairTime = time.Now().UnixNano()
		return dirAnyTime, r.saveDirMetadata(dirAnyTime, metadataAnyTime)
	}
	metadata.RecentRepairTime = time.Now().UnixNano()
	return dir, r.saveDirMetadata(dir, metadata)
}

// loadDirMetadata loads the directory metadata from disk
func (r *Renter) loadDirMetadata(path string) (dirMetadata, error) {
	var metadata dirMetadata
	err := persist.LoadJSON(dirMetadataHeader, &metadata, filepath.Join(path, SiaDirMetadata))
	if os.IsNotExist(err) {
		if err = r.createDirMetadata(path); err != nil {
			return metadata, err
		}
		return r.loadDirMetadata(path)
	}
	if err != nil {
		return metadata, err
	}
	return metadata, nil
}

// saveDirMetadata saves the directory metadata to disk
func (r *Renter) saveDirMetadata(path string, metadata dirMetadata) error {
	return persist.SaveJSON(dirMetadataHeader, metadata, filepath.Join(path, SiaDirMetadata))
}

// saveSync stores the current renter data to disk and then syncs to disk.
func (r *Renter) saveSync() error {
	return persist.SaveJSON(settingsMetadata, r.persist, filepath.Join(r.persistDir, PersistFilename))
}

// loadSiaFiles walks through the directory searching for siafiles and loading
// them into memory.
func (r *Renter) loadSiaFiles() error {
	// Recursively load all files found in renter directory. Errors
	// encountered during loading are logged, but are not considered fatal.
	return filepath.Walk(r.filesDir, func(path string, info os.FileInfo, err error) error {
		// This error is non-nil if filepath.Walk couldn't stat a file or
		// folder.
		if err != nil {
			r.log.Println("WARN: could not stat file or folder during walk:", err)
			return nil
		}

		// Skip folders and non-sia files.
		if info.IsDir() || filepath.Ext(path) != ShareExtension {
			return nil
		}

		// Load the Siafile.
		sf, err := siafile.LoadSiaFile(path, r.wal)
		if err != nil {
			// TODO try loading the file with the legacy format.
			r.log.Println("ERROR: could not open .sia file:", err)
			return nil
		}
		r.files[sf.SiaPath()] = sf
		return nil
	})
}

// managedCalculateMinHealth reads the sia files in the directory and returns the
// minHealth. This method with log errors and not consider errors fatal
func (r *Renter) managedCalculateMinHealth(path string) int {
	// Initialize minHealth as MaxInt64 so errors and empty directories won't
	// falsely indicate the most in need directory
	minHealth := math.MaxInt64

	// Read directory
	finfos, err := ioutil.ReadDir(path)
	if err != nil {
		r.log.Printf("WARN: Error in reading files in directory %v : %v\n", path, err)
		return minHealth
	}

	// Load siafiles
	var siafiles []*siafile.SiaFile
	for _, fi := range finfos {
		if !strings.HasPrefix(fi.Name(), ShareExtension) {
			continue
		}
		filename := filepath.Join(path, fi.Name())
		// Load the Siafile.
		sf, err := siafile.LoadSiaFile(filename, r.wal)
		if err != nil {
			continue
		}

		siafiles = append(siafiles, sf)
	}

	// Calculate file healths and find minHealth
	offline, _ := r.managedFileUtilities(siafiles)
	for _, sf := range siafiles {
		// Skip files that have recently been repaired
		if time.Since(sf.RecentRepairTime()) < repairInterval {
			continue
		}
		health := sf.Health(offline)
		if health < minHealth {
			minHealth = health
		}
	}
	return minHealth
}

// threadedUpdateRenterFileHealth reads all the sia files in the renter, calculates
// the health of each file and updates the folder metadata
func (r *Renter) threadedUpdateRenterFileHealth() {
	if err := r.tg.Add(); err != nil {
		return
	}
	defer r.tg.Done()

	for {
		// Work through the renter's file. File health will be calculated and
		// the min health will be stored in the directory metadata and bubbled
		// up to the top level file directory. When all the files have been
		// checked, we wait for the checkFileHealthSignal and start over.
		checkFileHealthSignal := time.After(healthCheckInterval)

		// Recursively read all files found in renter directory. Errors
		// encountered during loading are logged, but are not considered fatal.
		_ = filepath.Walk(r.filesDir, func(path string, info os.FileInfo, err error) error {
			// This error is non-nil if filepath.Walk couldn't stat a file or
			// folder.
			if err != nil {
				r.log.Println("WARN: could not stat file or folder during walk:", err)
				return nil
			}

			// Skip files.
			if !info.IsDir() {
				return nil
			}

			// Calculate health of directory files and find min health
			minHealth := r.managedCalculateMinHealth(path)

			// Update directory metadata
			md, err := r.loadDirMetadata(path)
			if err != nil {
				r.log.Printf("WARN: could not load directory metadata %v : %v\n", path, err)
			}
			md.MinHealth = minHealth
			md.RecentUpdateTime = time.Now().UnixNano()
			err = r.saveDirMetadata(path, md)
			if err != nil {
				r.log.Println("WARN: could not update directory metadata:", err)
				return nil
			}

			// Propagate the minHealth to make sure that the minHealth is properly
			// reflected through the renter directory
			r.propagateMinHealth(filepath.Dir(path), minHealth)

			return nil
		})

		// Block until work is required.
		select {
		case <-checkFileHealthSignal:
			// Time to check the filesystem health again.
		case <-r.tg.StopChan():
			// The renter has shutdown
			return
		}
	}
}

// propagateMinHealth make sure that the minHealth is properly reflected
// throughout the renter directory. This function will always compare the
// provided minHealth with the minHealth of the directory metadata so if a
// directory was just updated it should have its metadata updated prior to
// calling this function
func (r *Renter) propagateMinHealth(path string, minHealth int) {
	for path != r.persistDir {
		func() {
			defer func() {
				path = filepath.Dir(path)
			}()

			// Read directory metadata
			md, err := r.loadDirMetadata(path)
			if err != nil {
				r.log.Printf("WARN: Could not load directory metadata for %v: %v\n", path, err)
				return
			}

			// Check minHealth and update metadata if minHealth is less than
			// current directory minHealth
			if md.MinHealth <= minHealth {
				return
			}
			md.MinHealth = minHealth
			md.RecentUpdateTime = time.Now().UnixNano()
			err = r.saveDirMetadata(path, md)
			if err != nil {
				r.log.Println("WARN: could not update directory metadata:", err)
				return
			}
		}()
	}
}

// load fetches the saved renter data from disk.
func (r *Renter) loadSettings() error {
	r.persist = persistence{}
	err := persist.LoadJSON(settingsMetadata, &r.persist, filepath.Join(r.persistDir, PersistFilename))
	if os.IsNotExist(err) {
		// No persistence yet, set the defaults and continue.
		r.persist.MaxDownloadSpeed = DefaultMaxDownloadSpeed
		r.persist.MaxUploadSpeed = DefaultMaxUploadSpeed
		r.persist.StreamCacheSize = DefaultStreamCacheSize
		err = r.saveSync()
		if err != nil {
			return err
		}
	} else if err == persist.ErrBadVersion {
		// Outdated version, try the 040 to 133 upgrade.
		err = convertPersistVersionFrom040To133(filepath.Join(r.persistDir, PersistFilename))
		if err != nil {
			// Nothing left to try.
			return err
		}
		// Re-load the settings now that the file has been upgraded.
		return r.loadSettings()
	} else if err != nil {
		return err
	}

	// Set the bandwidth limits on the contractor, which was already initialized
	// without bandwidth limits.
	return r.setBandwidthLimits(r.persist.MaxDownloadSpeed, r.persist.MaxUploadSpeed)
}

// loadSharedFiles reads .sia data from reader and registers the contained
// files in the renter. It returns the nicknames of the loaded files.
func (r *Renter) loadSharedFiles(reader io.Reader, repairPath string) ([]string, error) {
	// read header
	var header [15]byte
	var version string
	var numFiles uint64
	err := encoding.NewDecoder(reader).DecodeAll(
		&header,
		&version,
		&numFiles,
	)
	if err != nil {
		return nil, err
	} else if header != shareHeader {
		return nil, ErrBadFile
	} else if version != shareVersion {
		return nil, ErrIncompatible
	}

	// Create decompressor.
	unzip, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	dec := encoding.NewDecoder(unzip)

	// Read each file.
	files := make([]*file, numFiles)
	for i := range files {
		files[i] = new(file)
		err := dec.Decode(files[i])
		if err != nil {
			return nil, err
		}

		// Make sure the file's name does not conflict with existing files.
		dupCount := 0
		origName := files[i].name
		for {
			_, exists := r.files[files[i].name]
			if !exists {
				break
			}
			dupCount++
			files[i].name = origName + "_" + strconv.Itoa(dupCount)
		}
	}

	// Add files to renter.
	names := make([]string, numFiles)
	for i, f := range files {
		sf, err := r.fileToSiaFile(f, repairPath)
		if err != nil {
			return nil, err
		}
		r.files[f.name] = sf
		names[i] = f.name
	}
	// TODO Save the file in the new format.
	return names, nil
}

// initPersist handles all of the persistence initialization, such as creating
// the persistence directory and starting the logger.
func (r *Renter) initPersist() error {
	// Create the persist and files directories if they do not yet exist.
	err := os.MkdirAll(r.filesDir, 0700)
	if err != nil {
		return err
	}

	// Initialize the logger.
	r.log, err = persist.NewFileLogger(filepath.Join(r.persistDir, logFile))
	if err != nil {
		return err
	}

	// Load the prior persistence structures.
	err = r.loadSettings()
	if err != nil {
		return err
	}

	// Initialize the writeaheadlog.
	txns, wal, err := writeaheadlog.New(filepath.Join(r.persistDir, walFile))
	if err != nil {
		return err
	}
	r.wal = wal

	// Apply unapplied wal txns.
	for _, txn := range txns {
		for _, update := range txn.Updates {
			if siafile.IsSiaFileUpdate(update) {
				if err := siafile.ApplyUpdates(update); err != nil {
					return errors.AddContext(err, "failed to apply SiaFile update")
				}
			}
		}
	}

	// Load the siafiles into memory.
	return r.loadSiaFiles()
}

// LoadSharedFiles loads a .sia file into the renter. It returns the nicknames
// of the loaded files.
func (r *Renter) LoadSharedFiles(filename string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return r.loadSharedFiles(file, filename)
}

// LoadSharedFilesASCII loads an ASCII-encoded .sia file into the renter. It
// returns the nicknames of the loaded files.
func (r *Renter) LoadSharedFilesASCII(asciiSia string) ([]string, error) {
	lockID := r.mu.Lock()
	defer r.mu.Unlock(lockID)

	dec := base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(asciiSia))
	return r.loadSharedFiles(dec, "")
}

// ShareFiles writes an .sia file to disk to be shared with others.
func (r *Renter) ShareFiles(paths []string, shareDest string) error {
	return errors.New("Not implemented for new format yet")
}

// ShareFilesASCII creates an ASCII-encoded '.sia' file.
func (r *Renter) ShareFilesASCII(paths []string) (asciiSia string, err error) {
	return "", errors.New("Not implemented for new format yet")
}

// convertPersistVersionFrom040to133 upgrades a legacy persist file to the next
// version, adding new fields with their default values.
func convertPersistVersionFrom040To133(path string) error {
	metadata := persist.Metadata{
		Header:  settingsMetadata.Header,
		Version: persistVersion040,
	}
	p := persistence{}

	err := persist.LoadJSON(metadata, &p, path)
	if err != nil {
		return err
	}
	metadata.Version = persistVersion133
	p.MaxDownloadSpeed = DefaultMaxDownloadSpeed
	p.MaxUploadSpeed = DefaultMaxUploadSpeed
	p.StreamCacheSize = DefaultStreamCacheSize
	return persist.SaveJSON(metadata, p, path)
}
