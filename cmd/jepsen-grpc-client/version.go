package main

import (
	"context"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
)

// Preserve observed versions as JSON integers. A transport failure may follow
// a committed write, so only an explicit mismatch is a definitive CAS failure.
func (c rpcClient) versionOperation(ctx context.Context, req request) response {
	if c.api != apiTransactional {
		return response{Type: outcomeFail, Error: "version operations require transactional API"}
	}
	if req.Op == "version-read" {
		reply, err := c.txn.Get(ctx, &pb.GetRequest{Key: []byte(req.Key), NeedCommitTs: true})
		if err != nil {
			return response{Type: outcomeInfo, Error: err.Error()}
		}
		if reply.GetVersion() == nil {
			return response{Type: outcomeInfo, Error: "server returned no version metadata"}
		}
		if reply.GetMissing() {
			return response{Type: outcomeOK, Missing: true}
		}
		// Keep malformed bytes and versions in a successful observation so
		// the checker reports the contract violation.
		return response{Type: outcomeOK, Version: reply.GetCommitTs(), Value: registerValue(reply.GetValue())}
	}
	if req.Value == nil || req.Expected == 0 {
		return response{Type: outcomeFail, Error: "a version and replacement are required"}
	}
	reply, err := c.txn.CompareAndSwap(ctx, &pb.CompareAndSwapRequest{
		Key: []byte(req.Key), ExpectedCommitTs: req.Expected, Value: []byte(strconv.FormatInt(*req.Value, 10)),
	})
	if err != nil {
		return response{Type: outcomeInfo, Error: err.Error()}
	}
	if reply.GetMismatch() {
		return response{Type: outcomeFail, Error: "version-mismatch"}
	}
	return response{Type: outcomeOK, Version: reply.GetNewCommitTs(), Value: *req.Value}
}

func registerValue(raw []byte) any {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return "invalid integer: " + string(raw)
	}
	return value
}
