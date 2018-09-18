package siafile

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"

	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/writeaheadlog"
)

type (
	// SiaFile is the disk format for files uploaded to the Sia network.  It
	// contains all the necessary information to recover a file from its hosts and
	// allows for easy constant-time updates of the file without having to read or
	// write the whole file.
	SiaFile struct {
		// staticMetadata is the mostly static staticMetadata of a SiaFile. The reserved
		// size of the staticMetadata on disk should always be a multiple of 4kib.
		// The staticMetadata is also the only part of the file that is JSON encoded
		// and can therefore be easily extended.
		staticMetadata metadata

		// pubKeyTable stores the public keys of the hosts this file's pieces are uploaded to.
		// Since multiple pieces from different chunks might be uploaded to the same host, this
		// allows us to deduplicate the rather large public keys.
		pubKeyTable []types.SiaPublicKey

		// staticChunks are the staticChunks the file was split into.
		staticChunks []chunk

		// utility fields. These are not persisted.
		deleted   bool
		mu        sync.RWMutex
		staticUID string

		// persistence related fields.
		siaFilePath string             // path to the .sia file
		wal         *writeaheadlog.WAL // the wal that is used for SiaFiles
	}

	// chunk represents a single chunk of a file on disk
	chunk struct {
		// erasure code settings.
		//
		// StaticErasureCodeType specifies the algorithm used for erasure coding
		// chunks. Available types are:
		//   0 - Invalid / Missing Code
		//   1 - Reed Solomon Code
		//
		// erasureCodeParams specifies possible parameters for a certain
		// StaticErasureCodeType. Currently params will be parsed as follows:
		//   Reed Solomon Code - 4 bytes dataPieces / 4 bytes parityPieces
		//
		StaticErasureCodeType   [4]byte              `json:"erasurecodetype"`
		StaticErasureCodeParams [8]byte              `json:"erasurecodeparams"`
		staticErasureCode       modules.ErasureCoder // not persisted, exists for convenience

		// ExtensionInfo is some reserved space for each chunk that allows us
		// to indicate if a chunk is special.
		ExtensionInfo [16]byte `json:"extensioninfo"`

		// Pieces are the Pieces of the file the chunk consists of.
		Pieces [][]Piece `json:"pieces"`
	}

	// Piece represents a single piece of a chunk on disk
	Piece struct {
		HostPubKey types.SiaPublicKey // public key of the host
		MerkleRoot crypto.Hash        // merkle root of the piece
	}
)

// initTinyFile finalizes the initialization of a 'tiny' SiaFile by copying the
// data into the SiaFile.
func initTinyFile(sf *SiaFile) error {
	// Set the tiny file flag.
	sf.staticMetadata.StaticTinyFile = true
	// Read the data.
	data, err := ioutil.ReadFile(sf.staticMetadata.LocalPath)
	if err != nil {
		return err
	}
	// Create an update for the header.
	headerUpdates, err := sf.saveHeader()
	if err != nil {
		return err
	}
	// Create an update for the content which is stored directly within the
	// SiaFile.
	contentUpdate := sf.createInsertUpdate(sf.staticMetadata.ChunkOffset, data)
	// Apply the updates.
	return sf.createAndApplyTransaction(append(headerUpdates, contentUpdate)...)
}

// New create a new SiaFile.
func New(siaFilePath, siaPath, source string, wal *writeaheadlog.WAL, erasureCode []modules.ErasureCoder, masterKey crypto.CipherKey, fileSize uint64, fileMode os.FileMode) (*SiaFile, error) {
	currentTime := time.Now()
	file := &SiaFile{
		staticMetadata: metadata{
			AccessTime:          currentTime,
			ChunkOffset:         defaultReservedMDPages * pageSize,
			ChangeTime:          currentTime,
			CreateTime:          currentTime,
			StaticFileSize:      int64(fileSize),
			LocalPath:           source,
			StaticMasterKey:     masterKey.Key(),
			StaticMasterKeyType: masterKey.Type(),
			Mode:                fileMode,
			ModTime:             currentTime,
			StaticPieceSize:     modules.SectorSize - masterKey.Type().Overhead(),
			SiaPath:             siaPath,
		},
		siaFilePath: siaFilePath,
		staticUID:   hex.EncodeToString(fastrand.Bytes(20)),
		wal:         wal,
	}
	// If the file is a tiny file we initialize it accordingly if the source
	// file is available on disk.
	if _, err := os.Stat(source); err == nil && fileSize <= TinyFileSize {
		return file, initTinyFile(file)
	}
	// Otherwise continue initializing a regular file.
	file.staticChunks = make([]chunk, len(erasureCode))

	// While initializing the chunks we count the data that is covered by the
	// chunk's ErasureCoder. That way we can sanity check if the file is
	// created with a sufficient number of ErasureCoders.
	coveredSize := uint64(0)
	for i := range file.staticChunks {
		if coveredSize >= fileSize {
			return nil, errors.New("more ErasureCoder than necessary were supplied")
		}
		// Add the size of one piece times the number of data pieces to the
		// coveredSize
		coveredSize += (modules.SectorSize - masterKey.Type().Overhead()) * uint64(erasureCode[i].MinPieces())

		// Init chunk.
		ecType, ecParams := marshalErasureCoder(erasureCode[i])
		file.staticChunks[i].staticErasureCode = erasureCode[i]
		file.staticChunks[i].StaticErasureCodeType = ecType
		file.staticChunks[i].StaticErasureCodeParams = ecParams
		file.staticChunks[i].Pieces = make([][]Piece, erasureCode[i].NumPieces())
	}
	// Check if the coveredSize is sufficient.
	if coveredSize < fileSize {
		return nil, fmt.Errorf("the supplied erasureCodes only cover %v out of %v bytes of the file",
			coveredSize, fileSize)
	}
	// Save file.
	return file, file.saveFile()
}

// AddPiece adds an uploaded piece to the file. It also updates the host table
// if the public key of the host is not already known.
func (sf *SiaFile) AddPiece(pk types.SiaPublicKey, chunkIndex, pieceIndex uint64, merkleRoot crypto.Hash) error {
	if sf.staticMetadata.StaticTinyFile {
		panic("AddPiece should never be called on a tiny file")
	}
	sf.mu.Lock()
	defer sf.mu.Unlock()
	// If the file was deleted we can't add a new piece since it would write
	// the file to disk again.
	if sf.deleted {
		return errors.New("can't add piece to deleted file")
	}

	// Get the index of the host in the public key table.
	tableIndex := -1
	for i, hpk := range sf.pubKeyTable {
		if hpk.Algorithm == pk.Algorithm && bytes.Equal(hpk.Key, pk.Key) {
			tableIndex = i
			break
		}
	}
	// If we don't know the host yet, we add it to the table.
	tableChanged := false
	if tableIndex == -1 {
		sf.pubKeyTable = append(sf.pubKeyTable, pk)
		tableIndex = len(sf.pubKeyTable) - 1
		tableChanged = true
	}
	// Check if the chunkIndex is valid.
	if chunkIndex >= uint64(len(sf.staticChunks)) {
		return fmt.Errorf("chunkIndex %v out of bounds (%v)", chunkIndex, len(sf.staticChunks))
	}
	// Check if the pieceIndex is valid.
	if pieceIndex >= uint64(len(sf.staticChunks[chunkIndex].Pieces)) {
		return fmt.Errorf("pieceIndex %v out of bounds (%v)", pieceIndex, len(sf.staticChunks[chunkIndex].Pieces))
	}
	// Add the piece to the chunk.
	sf.staticChunks[chunkIndex].Pieces[pieceIndex] = append(sf.staticChunks[chunkIndex].Pieces[pieceIndex], Piece{
		HostPubKey: pk,
		MerkleRoot: merkleRoot,
	})

	// Update the AccessTime, ChangeTime and ModTime.
	sf.staticMetadata.AccessTime = time.Now()
	sf.staticMetadata.ChangeTime = sf.staticMetadata.AccessTime
	sf.staticMetadata.ModTime = sf.staticMetadata.AccessTime

	// Update the file atomically.
	var updates []writeaheadlog.Update
	var err error
	// Get the updates for the header.
	if tableChanged {
		// If the table changed we update the whole header.
		updates, err = sf.saveHeader()
	} else {
		// Otherwise just the metadata.
		updates, err = sf.saveMetadata()
	}
	if err != nil {
		return err
	}
	// Get the updates for the chunks.
	// TODO Change this to update only a single chunk instead of all of them.
	chunksUpdate, err := sf.saveChunks()
	if err != nil {
		return err
	}
	return sf.createAndApplyTransaction(append(updates, chunksUpdate)...)
}

// Available indicates whether the file is ready to be downloaded.
func (sf *SiaFile) Available(offline map[string]bool) bool {
	if sf.TinyFile() {
		return true
	}
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	// We need to find at least erasureCode.MinPieces different pieces for each
	// chunk for the file to be available.
	for chunkIndex, chunk := range sf.staticChunks {
		piecesForChunk := 0
		for _, pieceSet := range chunk.Pieces {
			for _, piece := range pieceSet {
				if !offline[string(piece.HostPubKey.Key)] {
					piecesForChunk++
					break // break out since we only count unique pieces
				}
			}
			if piecesForChunk >= sf.staticChunks[chunkIndex].staticErasureCode.MinPieces() {
				break // we already have enough pieces for this chunk.
			}
		}
		if piecesForChunk < sf.staticChunks[chunkIndex].staticErasureCode.MinPieces() {
			return false // this chunk isn't available.
		}
	}
	return true
}

// ChunkIndexByOffset will return the chunkIndex that contains the provided
// offset of a file and also the relative offset within the chunk. If the
// offset is out of bounds, chunkIndex will be equal to NumChunk().
func (sf *SiaFile) ChunkIndexByOffset(offset uint64) (chunkIndex uint64, off uint64) {
	if sf.TinyFile() {
		panic("ChunkIndexByOffset should never be called on a tiny file")
	}
	for chunkIndex := uint64(0); chunkIndex < uint64(len(sf.staticChunks)); chunkIndex++ {
		if sf.staticChunkSize(chunkIndex) > offset {
			return chunkIndex, offset
		}
		offset -= sf.staticChunkSize(chunkIndex)
	}
	return
}

// ErasureCode returns the erasure coder used by the file.
func (sf *SiaFile) ErasureCode(chunkIndex uint64) modules.ErasureCoder {
	if sf.TinyFile() {
		panic("ErasureCode should never be called on a tiny file")
	}
	return sf.staticChunks[chunkIndex].staticErasureCode
}

// LoadTinyFileContent reads the content of a tiny file from the SiaFile. This
// should never be called on a regular file since regular files store their
// data on the Sia network.
func (sf *SiaFile) LoadTinyFileContent() ([]byte, error) {
	if !sf.TinyFile() {
		panic("LoadTinyFileContent should never be called on regular files")
	}
	// Load content.
	content, err := sf.readChunks()
	if err != nil {
		return nil, errors.AddContext(err, "failed to read tiny file's content from disk")
	}
	return content, nil
}

// NumChunks returns the number of chunks the file consists of. This will
// return the number of chunks the file consists of even if the file is not
// fully uploaded yet.
func (sf *SiaFile) NumChunks() uint64 {
	if sf.TinyFile() {
		panic("NumChunks should never be called on tiny files")
	}
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return uint64(len(sf.staticChunks))
}

// Pieces returns all the pieces for a chunk in a slice of slices that contains
// all the pieces for a certain index.
func (sf *SiaFile) Pieces(chunkIndex uint64) ([][]Piece, error) {
	if sf.TinyFile() {
		panic("Pieces should never be called on a tiny file.")
	}
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	if chunkIndex >= uint64(len(sf.staticChunks)) {
		panic(fmt.Sprintf("index %v out of bounds (%v)", chunkIndex, len(sf.staticChunks)))
	}
	// Return a deep-copy to avoid race conditions.
	pieces := make([][]Piece, len(sf.staticChunks[chunkIndex].Pieces))
	for pieceIndex := range pieces {
		pieces[pieceIndex] = make([]Piece, len(sf.staticChunks[chunkIndex].Pieces[pieceIndex]))
		copy(pieces[pieceIndex], sf.staticChunks[chunkIndex].Pieces[pieceIndex])
	}
	return pieces, nil
}

// Redundancy returns the redundancy of the least redundant chunk. A file
// becomes available when this redundancy is >= 1. Assumes that every piece is
// unique within a file contract. -1 is returned if the file has size 0. It
// takes two arguments, a map of offline contracts for this file and a map that
// indicates if a contract is goodForRenew.
func (sf *SiaFile) Redundancy(offlineMap map[string]bool, goodForRenewMap map[string]bool) float64 {
	// A tiny file always has redundancy 1x.
	if sf.TinyFile() {
		return 1.0
	}

	sf.mu.RLock()
	defer sf.mu.RUnlock()

	minRedundancy := math.MaxFloat64
	minRedundancyNoRenew := math.MaxFloat64
	for _, chunk := range sf.staticChunks {
		// Loop over chunks and remember how many unique pieces of the chunk
		// were goodForRenew and how many were not.
		numPiecesRenew := uint64(0)
		numPiecesNoRenew := uint64(0)
		for _, pieceSet := range chunk.Pieces {
			// Remember if we encountered a goodForRenew piece or a
			// !goodForRenew piece that was at least online.
			foundGoodForRenew := false
			foundOnline := false
			for _, piece := range pieceSet {
				offline, exists1 := offlineMap[string(piece.HostPubKey.Key)]
				goodForRenew, exists2 := goodForRenewMap[string(piece.HostPubKey.Key)]
				if exists1 != exists2 {
					build.Critical("contract can't be in one map but not in the other")
				}
				if !exists1 || offline {
					continue
				}
				// If we found a goodForRenew piece we can stop.
				if goodForRenew {
					foundGoodForRenew = true
					break
				}
				// Otherwise we continue since there might be other hosts with
				// the same piece that are goodForRenew. We still remember that
				// we found an online piece though.
				foundOnline = true
			}
			if foundGoodForRenew {
				numPiecesRenew++
				numPiecesNoRenew++
			} else if foundOnline {
				numPiecesNoRenew++
			}
		}
		redundancy := float64(numPiecesRenew) / float64(chunk.staticErasureCode.MinPieces())
		if redundancy < minRedundancy {
			minRedundancy = redundancy
		}
		redundancyNoRenew := float64(numPiecesNoRenew) / float64(chunk.staticErasureCode.MinPieces())
		if redundancyNoRenew < minRedundancyNoRenew {
			minRedundancyNoRenew = redundancyNoRenew
		}
	}

	// If the redundancy is smaller than 1x we return the redundancy that
	// includes contracts that are not good for renewal. The reason for this is
	// a better user experience. If the renter operates correctly, redundancy
	// should never go above numPieces / minPieces and redundancyNoRenew should
	// never go below 1.
	if minRedundancy < 1 && minRedundancyNoRenew >= 1 {
		return 1
	} else if minRedundancy < 1 {
		return minRedundancyNoRenew
	}
	return minRedundancy
}

// UID returns a unique identifier for this file.
func (sf *SiaFile) UID() string {
	return sf.staticUID
}
