package proto

import (
	"net"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/ratelimit"
)

// A Session is an ongoing exchange of RPCs via the renter-host protocol.
type Session struct {
	closeChan   chan struct{}
	conn        net.Conn
	contractID  types.FileContractID
	contractSet *ContractSet
	hdb         hostDB
	height      types.BlockHeight
	host        modules.HostDBEntry
	once        sync.Once
}

// Upload calls the Upload RPC and transfers the supplied data, returning the
// updated contract and the Merkle root of the sector.
func (s *Session) Upload(data []byte) (_ modules.RenterContract, _ crypto.Hash, err error) {
	// Acquire the contract.
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return modules.RenterContract{}, crypto.Hash{}, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	contract := sc.header // for convenience

	// calculate price
	// TODO: height is never updated, so we'll wind up overpaying on long-running uploads
	blockBytes := types.NewCurrency64(modules.SectorSize * uint64(contract.LastRevision().NewWindowEnd-s.height))
	sectorStoragePrice := s.host.StoragePrice.Mul(blockBytes)
	sectorBandwidthPrice := s.host.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorCollateral := s.host.Collateral.Mul(blockBytes)

	// to mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by 0.2%.
	sectorStoragePrice = sectorStoragePrice.MulFloat(1 + hostPriceLeeway)
	sectorBandwidthPrice = sectorBandwidthPrice.MulFloat(1 + hostPriceLeeway)
	sectorCollateral = sectorCollateral.MulFloat(1 - hostPriceLeeway)

	// check that enough funds are available
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	if contract.RenterFunds().Cmp(sectorPrice) < 0 {
		return modules.RenterContract{}, crypto.Hash{}, errors.New("contract has insufficient funds to support upload")
	}
	if contract.LastRevision().NewMissedProofOutputs[1].Value.Cmp(sectorCollateral) < 0 {
		return modules.RenterContract{}, crypto.Hash{}, errors.New("contract has insufficient collateral to support upload")
	}

	// calculate the new Merkle root
	sectorRoot := crypto.MerkleRoot(data)
	merkleRoot := sc.merkleRoots.checkNewRoot(sectorRoot)

	// create the revision and sign it
	rev := newUploadRevision(contract.LastRevision(), merkleRoot, sectorPrice, sectorCollateral)
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), contract.SecretKey)
	txn.TransactionSignatures[0].Signature = sig[:]

	// create the request
	req := modules.LoopUploadRequest{
		ContractID:        contract.ID(),
		Data:              data,
		NewRevisionNumber: rev.NewRevisionNumber,
		Signature:         sig[:],
	}
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}

	// record the change we are about to make to the contract. If we lose power
	// mid-revision, this allows us to restore either the pre-revision or
	// post-revision contract.
	walTxn, err := sc.recordUploadIntent(rev, sectorRoot, sectorStoragePrice, sectorBandwidthPrice)
	if err != nil {
		return modules.RenterContract{}, crypto.Hash{}, err
	}

	defer func() {
		// Increase Successful/Failed interactions accordingly
		if err != nil {
			s.hdb.IncrementFailedInteractions(s.host.PublicKey)
		} else {
			s.hdb.IncrementSuccessfulInteractions(s.host.PublicKey)
		}

		// reset deadline
		extendDeadline(s.conn, time.Hour)
	}()

	// send download RPC request
	extendDeadline(s.conn, 2*time.Minute) // TODO: Constant.
	err = encoding.NewEncoder(s.conn).EncodeAll(modules.RPCLoopUpload, req)
	if err != nil {
		return modules.RenterContract{}, crypto.Hash{}, err
	}

	// read the response
	var resp modules.LoopUploadResponse
	err = modules.ReadRPCResponse(s.conn, &resp)
	if err != nil {
		return modules.RenterContract{}, crypto.Hash{}, err
	}

	// add host signature
	txn.TransactionSignatures[1].Signature = resp.Signature

	// update contract
	err = sc.commitUpload(walTxn, txn, sectorRoot, sectorStoragePrice, sectorBandwidthPrice)
	if err != nil {
		return modules.RenterContract{}, crypto.Hash{}, err
	}

	return sc.Metadata(), sectorRoot, nil
}

// Download calls the download RPC and returns the requested data. The
// Revision and Signature fields of req are filled in automatically. If a
// Merkle proof is requested, it is verified.
func (s *Session) Download(req modules.LoopDownloadRequest) (_ modules.RenterContract, _ []byte, err error) {
	// Reset deadline when finished.
	defer extendDeadline(s.conn, time.Hour) // TODO: Constant.

	// Sanity-check the request.
	if req.MerkleProof {
		if req.Offset%crypto.SegmentSize != 0 || req.Length%crypto.SegmentSize != 0 {
			return modules.RenterContract{}, nil, errors.New("offset and length must be multiples of SegmentSize when requesting a Merkle proof")
		}
	} else if uint64(req.Length) != modules.SectorSize {
		return modules.RenterContract{}, nil, errors.New("must request Merkle proof when downloading less than a full sector")
	}

	// Acquire the contract.
	// TODO: why not just lock the SafeContract directly?
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return modules.RenterContract{}, nil, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	contract := sc.header // for convenience

	// calculate price
	price := s.host.DownloadBandwidthPrice.Mul64(uint64(req.Length))
	if contract.RenterFunds().Cmp(price) < 0 {
		return modules.RenterContract{}, nil, errors.New("contract has insufficient funds to support download")
	}
	// To mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by 0.2%.
	price = price.MulFloat(1 + hostPriceLeeway)

	// create the download revision and sign it
	rev := newDownloadRevision(contract.LastRevision(), price)
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), contract.SecretKey)
	txn.TransactionSignatures[0].Signature = sig[:]

	// fill in the missing request fields
	req.ContractID = contract.ID()
	req.NewRevisionNumber = rev.NewRevisionNumber
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}
	req.Signature = sig[:]

	// record the change we are about to make to the contract. If we lose power
	// mid-revision, this allows us to restore either the pre-revision or
	// post-revision contract.
	walTxn, err := sc.recordDownloadIntent(rev, price)
	if err != nil {
		return modules.RenterContract{}, nil, err
	}

	// Increase Successful/Failed interactions accordingly
	defer func() {
		if err != nil {
			s.hdb.IncrementFailedInteractions(contract.HostPublicKey())
		} else {
			s.hdb.IncrementSuccessfulInteractions(contract.HostPublicKey())
		}
	}()

	// send download RPC request
	extendDeadline(s.conn, 2*time.Minute) // TODO: Constant.
	err = encoding.NewEncoder(s.conn).EncodeAll(modules.RPCLoopDownload, req)
	if err != nil {
		return modules.RenterContract{}, nil, err
	}

	// read the response
	var resp modules.LoopDownloadResponse
	err = modules.ReadRPCResponse(s.conn, &resp)
	if err != nil {
		return modules.RenterContract{}, nil, err
	}

	if len(resp.Data) != int(req.Length) {
		return modules.RenterContract{}, nil, errors.New("host did not send enough sector data")
	} else if req.MerkleProof {
		proofStart := int(req.Offset) / crypto.SegmentSize
		proofEnd := int(req.Offset+req.Length) / crypto.SegmentSize
		if !crypto.VerifyRangeProof(resp.Data, resp.MerkleProof, proofStart, proofEnd, req.MerkleRoot) {
			return modules.RenterContract{}, nil, errors.New("host provided incorrect sector data or Merkle proof")
		}
	} else if !crypto.MerkleRoot(resp.Data) != req.MerkleProof {
		return modules.RenterContract{}, nil, errors.New("host provided incorrect sector data")
	}

	// add host signature
	txn.TransactionSignatures[1].Signature = resp.Signature

	// update contract and metrics
	if err := sc.commitDownload(walTxn, txn, price); err != nil {
		return modules.RenterContract{}, nil, err
	}

	return sc.Metadata(), resp.Data, nil
}

// shutdown terminates the revision loop and signals the goroutine spawned in
// NewSession to return.
func (s *Session) shutdown() {
	extendDeadline(s.conn, modules.NegotiateSettingsTime)
	// don't care about this error
	_ = encoding.NewEncoder(s.conn).EncodeAll(modules.RPCLoopExit)
	close(s.closeChan)
}

// Close cleanly terminates the download loop with the host and closes the
// connection.
func (s *Session) Close() error {
	// using once ensures that Close is idempotent
	s.once.Do(s.shutdown)
	return s.conn.Close()
}

// NewSession initiates the RPC loop with a host and returns a Session.
func (cs *ContractSet) NewSession(host modules.HostDBEntry, id types.FileContractID, currentHeight types.BlockHeight, hdb hostDB, cancel <-chan struct{}) (_ *Session, err error) {
	sc, ok := cs.Acquire(id)
	if !ok {
		return nil, errors.New("invalid contract")
	}
	defer cs.Return(sc)
	contract := sc.header

	// Increase Successful/Failed interactions accordingly
	defer func() {
		// A revision mismatch might not be the host's fault.
		if err != nil && !IsRevisionMismatch(err) {
			hdb.IncrementFailedInteractions(contract.HostPublicKey())
			err = errors.Extend(err, modules.ErrHostFault)
		} else if err == nil {
			hdb.IncrementSuccessfulInteractions(contract.HostPublicKey())
		}
	}()

	c, err := (&net.Dialer{
		Cancel:  cancel,
		Timeout: 45 * time.Second, // TODO: Constant
	}).Dial("tcp", string(host.NetAddress))
	if err != nil {
		return nil, err
	}
	conn := ratelimit.NewRLConn(c, cs.rl, cancel)

	closeChan := make(chan struct{})
	go func() {
		select {
		case <-cancel:
			conn.Close()
		case <-closeChan:
		}
	}()

	extendDeadline(conn, modules.NegotiateSettingsTime)
	if err := encoding.WriteObject(conn, modules.RPCLoopEnter); err != nil {
		return nil, err
	}

	// if we succeeded, we can safely discard the unappliedTxns
	for _, txn := range sc.unappliedTxns {
		txn.SignalUpdatesApplied()
	}
	sc.unappliedTxns = nil

	// the host is now ready to accept revisions
	return &Session{
		closeChan:   closeChan,
		conn:        conn,
		contractID:  id,
		contractSet: cs,
		hdb:         hdb,
		height:      currentHeight,
		host:        host,
	}, nil
}
