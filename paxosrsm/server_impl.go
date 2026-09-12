package paxosrsm

import (
	"umich.edu/eecs491/proj4/paxos"
	"time"
)

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	maxSeqDone int
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.maxSeqDone = -1
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}, equals func(v1 interface{}, v2 interface{}) bool) { //v is an opstruct here
	newSeq := rsm.impl.maxSeqDone + 1
	var decided bool

	for !decided{
		to := 10 * time.Millisecond
		rsm.px.Start(newSeq, v)
		for {
			fate, val := rsm.px.Status(newSeq)
			
			if(fate == paxos.Decided){
				rsm.applyOp(val) //sends to the kv layer
				rsm.impl.maxSeqDone = newSeq
				rsm.px.Done(rsm.impl.maxSeqDone)
				//finished up until this pt
				if(equals(val, v)){
					//we are done
					decided = true
					//application layer has finished 
					//everything until this point
					break
				}
				newSeq++ //try again for the next sequence
				break
			} else{
				time.Sleep(to)
				if to < 500 * time.Millisecond {
					to *= 2
				}
			}
		}
	}
}


//comments:
//need to start a Paxos instance here ==> do we assign it a sequence number?
//return when we get consensus
//start an instance & continuously check status of that instance
//once value has been decided, send value back to interface using ApplyOp() RPC call
//check the log for duplicates ==> return that value
//if not a duplicate it can call start on RSM layer
//only returns once the function is fully done
//check if already exists in our slots (duplicate client request)
//get our max sequence number from the paxos layer