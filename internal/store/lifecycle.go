package store

import (
	"context"
	"database/sql"
	"fmt"
)

// The data lifecycle's state machine (README: Data stores, the state table):
// the states of each record Kairo keeps of data, and every change of state
// the store makes. Each place that changes one checks the change here
// (checkTransition, before its UPDATE, INSERT or DELETE), so a path that
// makes a change the machine does not have fails, saying so, rather than
// leaving a record where no other path expects it. The UPDATE's own WHERE
// stays what decides between concurrent changes.

// lifecycleRecord is a record with a lifecycle state.
type lifecycleRecord string

const (
	recObject    lifecycleRecord = "object"     // data_objects.state
	recSource    lifecycleRecord = "source"     // data_objects.source_state
	recTransfer  lifecycleRecord = "transfer"   // data_objects.transfer_state
	recCopy      lifecycleRecord = "copy"       // object_copies.state
	recPlacement lifecycleRecord = "placement"  // object_placements.state
	recScratch   lifecycleRecord = "scratch"    // attempt_scratch.state
	recRunOutput lifecycleRecord = "run output" // run_outputs.state
)

// noRecord is the state of a record not there: made from it (an INSERT),
// removed to it (a DELETE, or a column set NULL).
const noRecord = ""

// lifecycle is, for each record, the states a state can change to.
var lifecycle = map[lifecycleRecord]map[string][]string{
	recObject: {
		// ordered packed (PacksToMake, ImportData, run snapshots), or
		// adopted from a durable store's description (AdoptCopies)
		noRecord:    {"packing", "packed"},
		"packing":   {"packed", "abandoned"},
		"abandoned": {"packed"}, // a pack being made when its import was cancelled is still recorded
		"packed":    {},
	},
	recSource: {
		noRecord: {SourcePresent, SourceKept}, // made with its object; adopted kept
		SourcePresent: {SourceDeleting, SourceKept, SourceEvicting,
			SourceDeleted}, // deleted: found gone from its path (InputGone)
		SourceDeleting: {SourceDeleted, SourceKept},
		SourceEvicting: {SourceEvicted, SourcePresent},
		SourceKept:     {SourceDeleted}, // a kept import found gone from its path
		SourceDeleted:  {SourceKept},    // an import abandoned once its source was found gone
		SourceEvicted:  {},
	},
	recTransfer: {
		noRecord:         {transferPacking},
		transferPacking:  {transferPacked},
		transferPacked:   {transferDropping},
		transferDropping: {noRecord},
	},
	recCopy: {
		noRecord:   {"copying", "verified"}, // ordered; adopted from a durable store
		"copying":  {"verified", noRecord},  // given up (its store no longer durable there)
		"verified": {noRecord},              // shed
	},
	recPlacement: {
		noRecord:   {"fetching"},
		"fetching": {"ready"},
		"ready":    {"evicting", noRecord}, // forgotten: found gone from its path
		"evicting": {"ready", noRecord},    // kept, or evicted
	},
	recScratch: {
		noRecord:        {"active"},
		"active":        {ScratchDeleting, "kept", "missing"},
		"missing":       {ScratchDeleting, "kept", "active"}, // settled while missing, or found again
		ScratchDeleting: {"deleted", "kept"},
		"kept":          {"missing", "deleted"}, // deleted once copied to a durable store
		"deleted":       {},
	},
	recRunOutput: {
		noRecord:  {"active"},
		"active":  {"missing", "deleted"},
		"missing": {"active", "deleted"},
		"deleted": {},
	},
}

// The states of a source (data_objects.source_state).
const (
	SourcePresent  = "present"
	SourceEvicting = "evicting"
	SourceEvicted  = "evicted"
)

// checkTransition refuses a change of a record's state the lifecycle does
// not have; staying in a state is no change.
func checkTransition(r lifecycleRecord, from, to string) error {
	if from == to {
		return nil
	}
	states, ok := lifecycle[r]
	if !ok {
		return fmt.Errorf("no lifecycle for %s", r)
	}
	for _, s := range states[from] {
		if s == to {
			return nil
		}
	}
	return fmt.Errorf("%s from %q to %q is no change the data lifecycle makes", r, from, to)
}

// checkTransitions is checkTransition from each of froms: a change an
// UPDATE makes of the rows its WHERE selects in any of those states.
func checkTransitions(r lifecycleRecord, froms []string, to string) error {
	for _, from := range froms {
		if err := checkTransition(r, from, to); err != nil {
			return err
		}
	}
	return nil
}

// checkRowsTransitionTx checks the change to to of the rows of table its
// where (with args) selects, as they are now (locked until tx ends).
func checkRowsTransitionTx(ctx context.Context, tx *sql.Tx, r lifecycleRecord, table, where string, to string, args ...any) error {
	rows, err := tx.QueryContext(ctx, `SELECT state FROM `+table+` WHERE `+where+` FOR UPDATE`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var from string
		if err := rows.Scan(&from); err != nil {
			return err
		}
		if err := checkTransition(r, from, to); err != nil {
			return err
		}
	}
	return rows.Err()
}

// checkMade checks a record made in state to; an object is made with its
// source present.
func checkMade(r lifecycleRecord, to string) error {
	err := checkTransition(r, noRecord, to)
	if r == recObject && err == nil {
		err = checkTransition(recSource, noRecord, SourcePresent)
	}
	return err
}

// checkAdopted checks what adopting a copy found on a durable store makes
// (AdoptCopies): its object packed, the object's source kept, the copy
// verified.
func checkAdopted() error {
	for _, c := range []struct {
		r  lifecycleRecord
		to string
	}{{recObject, "packed"}, {recSource, SourceKept}, {recCopy, "verified"}} {
		if err := checkTransition(c.r, noRecord, c.to); err != nil {
			return err
		}
	}
	return nil
}
