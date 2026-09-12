package paxos

import "umich.edu/eecs491/proj4/common"

import "time"
import "math/rand"

//
// additions to Paxos state.
//
const MaxInt = int(^uint(0) >> 1)

type PaxosInstance struct{
	n_p int
	n_a int
	v_a interface{}
	Fate Fate
	finalVal interface{}
}

type startReq struct{
	seq int
	value interface{}
}

type statusReq struct{
	seq int
	responseChan chan(Status)
}

type Status struct{
	fate Fate
	value interface{}
}

type doneReq  struct{
	seq int 
}

type peerDoneReq  struct{
	peerID int 
	doneNum int
}

type highestSeqDoneReq struct{
	responseChan chan(int)
}

type informReq struct{
	informArg InformArgs
	responseChan chan(InformReply) //lwk just using to acknowledge here
}

type prepareReq  struct{
	prepareArg PrepareArgs
	responseChan chan(PrepareReply)
}

type acceptReq struct{
	acceptArg AcceptArgs
	responseChan chan(AcceptReply)
}

type maxReq struct{
	responseChan chan(int)
}

type minReq struct{
	responseChan chan(int)
}

type PaxosImpl struct {
	//goroutine that runs the algorithm
	//reads from a Chan of serialized requests from this server 
	//state that we need to know:

	//maps
	slotToPaxos     map[int]*PaxosInstance
	doneMap 		map[int]int
	startMap 		map[int]int

	//channels
	startChan       chan startReq
	doneChan        chan doneReq
	peerDoneChan	chan peerDoneReq
	statusChan      chan statusReq
	prepareChan     chan prepareReq
	acceptChan      chan acceptReq
	maxChan         chan maxReq
	highestSeqDoneChan chan highestSeqDoneReq
	informChan      chan informReq
	minChan 		chan minReq
	highestSeqDone int
	maxKnownSequence int
}

//
// your px.impl.* initializations here.
//
func (px *Paxos) initImpl() {
	//maps
	px.impl.slotToPaxos    = make(map[int]*PaxosInstance)
	px.impl.doneMap		   = make(map[int]int)
	px.impl.startMap	   = make(map[int]int)
	
	//channels
	px.impl.startChan      = make(chan startReq, 1000)
	px.impl.doneChan       = make(chan doneReq, 1000)
	px.impl.peerDoneChan   = make(chan peerDoneReq, 1000)
	px.impl.statusChan     = make(chan statusReq, 1000)
	px.impl.prepareChan    = make(chan prepareReq, 1000)
	px.impl.acceptChan     = make(chan acceptReq, 1000)
	px.impl.maxChan        = make(chan maxReq, 1000)
	px.impl.informChan     = make(chan informReq, 1000)
	px.impl.minChan        = make(chan minReq, 1000)
	px.impl.highestSeqDoneChan = make(chan highestSeqDoneReq, 1000)

	px.impl.highestSeqDone = -1
	px.impl.maxKnownSequence = -1
	//event loop
	go px.eventLoop();
}


func (px *Paxos) eventLoop(){ //this loop has shared state over the whole map
	//need a highest number done
	for{
		select{
		case startReq := <- px.impl.startChan:	
			
			minSeq := px.findMin()
			minVal := minSeq + 1

			inst, ok := px.impl.slotToPaxos[startReq.seq]
			alreadyDecided := ok && inst.Fate == Decided
			tooOld := startReq.seq < minVal
			_, alreadyStarted := px.impl.startMap[startReq.seq]
			
			if(startReq.seq > px.impl.maxKnownSequence){
				px.impl.maxKnownSequence = startReq.seq
			}

			if(!alreadyDecided && !tooOld && !alreadyStarted){
				//add to map
				px.impl.startMap[startReq.seq] = 1
				go px.doPaxos(startReq.seq, startReq.value)
			}
			
		case prepareReq := <- px.impl.prepareChan:
			if(prepareReq.prepareArg.Seq > px.impl.maxKnownSequence){
				px.impl.maxKnownSequence = prepareReq.prepareArg.Seq
			}
			reply := px.handlePaxosRPC(
				false, 
				prepareReq.prepareArg.Seq, 
				prepareReq.prepareArg.N,
				nil,
			)
			prepareReq.responseChan <- reply
			
		case accReq := <- px.impl.acceptChan:
			if(accReq.acceptArg.Seq > px.impl.maxKnownSequence){
				px.impl.maxKnownSequence = accReq.acceptArg.Seq
			}
			reply := px.handlePaxosRPC(
				true, 
				accReq.acceptArg.Seq, 
				accReq.acceptArg.N,
				accReq.acceptArg.Value,
			)
			accReq.responseChan <- AcceptReply(reply)
			
		case informReq := <- px.impl.informChan:
			//just need to set our values
			inst, ok := px.impl.slotToPaxos[informReq.informArg.Seq]
			if !ok {
				inst = newPaxosInstance()
				px.impl.slotToPaxos[informReq.informArg.Seq] = inst
			}
			if(informReq.informArg.Seq > px.impl.maxKnownSequence){
				px.impl.maxKnownSequence = informReq.informArg.Seq
			}

			if(inst.Fate != Decided){
				inst.v_a = informReq.informArg.Value
				inst.finalVal = informReq.informArg.Value
				inst.Fate = Decided
			}
			
			//make an informResponse
			resp := InformReply{}
			resp.Done = px.impl.highestSeqDone
			resp.PeerID = px.me
			resp.Reply = OK
			informReq.responseChan <- resp
			
		case maxReq := <- px.impl.maxChan:
			maxReq.responseChan <- px.impl.maxKnownSequence
		
		case statusReq := <- px.impl.statusChan:
			//find status by iterating through map
			statusReturn := Pending
			var valReturn interface{}
			
			minSeq := px.findMin()

			if statusReq.seq < minSeq + 1{
				statusReq.responseChan <- Status{
					fate: Forgotten,
					value: nil,
				}
				break
			}

			inst, ok := px.impl.slotToPaxos[statusReq.seq]
			if ok && inst.Fate == Decided{
				statusReturn = Decided
				valReturn = inst.finalVal
			}

			statusReq.responseChan <- Status{
				fate:  statusReturn,
				value: valReturn,
			}
		
		case doneReq := <- px.impl.doneChan: //lowkey should DRAIN from this channel?!
			best := doneReq.seq
			for {
				select {
				case d := <-px.impl.doneChan:
					if d.seq > best {
						best = d.seq
					}
				default:
					goto doneDrain
				}
			}
		doneDrain:
			if best > px.impl.highestSeqDone {
				px.impl.highestSeqDone = best
			}
			px.impl.doneMap[px.me] = px.impl.highestSeqDone
		
		case highestNumReq := <- px.impl.highestSeqDoneChan:
			highestNumReq.responseChan <- px.impl.highestSeqDone
		
		case peerDoneReq := <-px.impl.peerDoneChan:
			cur, ok := px.impl.doneMap[peerDoneReq.peerID]
			if !ok {
				cur = -1
			}
			if peerDoneReq.doneNum > cur {
				px.impl.doneMap[peerDoneReq.peerID] = peerDoneReq.doneNum
			}

		peerDoneDrain:
			for {
				select {
				case p := <-px.impl.peerDoneChan:
					cur, ok := px.impl.doneMap[p.peerID]
					if !ok {
						cur = -1
					}
					if p.doneNum > cur {
						px.impl.doneMap[p.peerID] = p.doneNum
					}
				default:
					break peerDoneDrain
				}
			}
			
			minSeq := px.findMin()

			//delete all the values for this sequence number in the map
			for key := range(px.impl.slotToPaxos){
				if(key <= minSeq){
					delete(px.impl.slotToPaxos, key)
					delete(px.impl.startMap, key)
				}
			}

		case minReq := <- px.impl.minChan:
			minSeq := px.findMin()
			minReq.responseChan <- minSeq + 1

		case <- px.term:
			return
		}
	}
}

func (px *Paxos) handlePaxosRPC(isAccept bool, seq int, n int, value interface{}) PrepareReply{
	inst, ok := px.impl.slotToPaxos[seq] //want a reference to this in the map
	if !ok {
		inst = newPaxosInstance()
		px.impl.slotToPaxos[seq] = inst
	}

	reply := PrepareReply{}
	if(isAccept && inst.Fate == Decided){
		if(n >= inst.n_p){ 
			reply.Reply = OK
		} else{
			reply.Reply = Reject
		}
	} else if (!isAccept && n > inst.n_p) || (isAccept && n >= inst.n_p){
		reply.Reply = OK
		inst.n_p = n
		if(isAccept){
			inst.n_a = n
			inst.v_a = value
		}
	} else{
		reply.Reply = Reject
	}
	
	//return the values:
	reply.N_p = inst.n_p
	reply.N_a = inst.n_a
	reply.V_a = inst.v_a
	reply.Done = px.impl.highestSeqDone
	reply.PeerID = px.me
	return reply
}



func (px *Paxos) doPaxos(seq int, v interface{}){
	//proposer algorithm
	curHighestN := 0
	numPeers := len(px.peers)
	majorityThreshold := numPeers / 2 + 1
	backoff := 5
	maxBackoff := 250 
	var decidedVal interface{}

	for !px.isdead(){ //keep looping unless we're killed
		
		if(px.alreadyDecided(seq)){
			return
		}

		counter := (curHighestN / numPeers) + 1
		n := (counter*numPeers) + px.me
		curHighestN = n

		//make a call to Done and get that arg
		done := px.getDoneVal()

		prepareArg := PrepareArgs{
			Seq: seq,
			N: n,
			PeerID: px.me,
			Done: done,
		}

		var newV interface{}
		highestN_a := -1
		var mostPrepareAccepted map[int]int = make(map[int]int)
		var ballotToValue map[int]interface{} = make(map[int]interface{})

		numPrepare := 0
		
		//preparePhase
		for i := 0; i < numPeers; i++ {
			r := &PrepareReply{}
			ok := true
			if(i == px.me){
				px.Prepare(&prepareArg, r)
			} else{
				ok = common.Call(px.peers[i], "Paxos.Prepare", &prepareArg, r)
			}
			if ok{
				if r.Reply == OK{
					numPrepare++
				}
				if r.N_p > curHighestN{
					curHighestN = r.N_p
				}

				if r.V_a != nil {
					if r.Reply == OK && r.N_a > highestN_a {
						highestN_a = r.N_a
						newV = r.V_a
					}

					mostPrepareAccepted[r.N_a]++ //to track if we can exit the accept state
					if _, ok := ballotToValue[r.N_a]; !ok {
    					ballotToValue[r.N_a] = r.V_a
					}
				}

				//send doneValue to peerDone
				px.sendPeerInfo(r.PeerID, r.Done)
			}
		}

		//if a value has already been decided by a majority, then don't need to run accept
		maxFreq := 0
		maxBallot := 0

		for key, freq := range mostPrepareAccepted {
			if  freq > maxFreq {
				maxFreq = freq
				maxBallot = key
			}
		}

		if(maxFreq >= majorityThreshold){ //value has already been decided, same ballot is a majority
			decidedVal = ballotToValue[maxBallot]
			break
		}

		if(numPrepare >= majorityThreshold){
			//check the status again
			if(px.alreadyDecided(seq)){
				return
			}

			//can run accept alg, should learn the value if rejected
			if(highestN_a == -1){
				newV = v //keep our current value
			}

			//run accept now
			done = px.getDoneVal()

			acceptArg := AcceptArgs{
				Seq: seq,
				N: n,
				Value: newV,
				PeerID: px.me,
				Done: done,
			}

			//broadcast numAccept RPC
			//send Accept to myself
			numAccept := 0
			for i := 0; i < numPeers; i++ {
				ok := true
				r := &AcceptReply{}
				if(i == px.me){
					px.Accept(&acceptArg, r)
				} else{
					ok = common.Call(px.peers[i], "Paxos.Accept", &acceptArg, r)
				}
				if ok{
					if(r.Reply == OK){
						numAccept++
					}

					if r.N_p > curHighestN{
						curHighestN = r.N_p
					}

					px.sendPeerInfo(r.PeerID, r.Done)
				}
			}

			//we decided the value
			if (numAccept >= majorityThreshold){
				decidedVal = newV
				break
			}
		}

		jitter := rand.Intn(backoff)
		delay := time.Duration(jitter) * time.Millisecond
		time.Sleep(delay)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	//inform phase
	//send a finish slot request
	//if already decided, return
	if(px.alreadyDecided(seq)){
		return
	}

	done := px.getDoneVal()

	informArg := InformArgs{
		Seq: seq,
		Value: decidedVal,
		PeerID: px.me,
		Done: done,
	}

	myInformRep := InformReply{}
	px.Inform(&informArg, &myInformRep) //inform myself

	for i := 0; i < numPeers; i++{
		if i != px.me {
			informRep := InformReply{}
			ok := common.Call(px.peers[i], "Paxos.Inform", &informArg, &informRep)
			if ok{
				//send informRep done argument to our done channel
				px.sendPeerInfo(informRep.PeerID, informRep.Done)
			}
		}
	}
}

func newPaxosInstance() *PaxosInstance {
	return &PaxosInstance{
		n_p:      -1,
		n_a:      -1,
		v_a:      nil,
		Fate:     Pending,
		finalVal: nil,
	}
}

func (px *Paxos) findMin() int{
	minSeq := MaxInt

	for i := 0; i < len(px.peers); i++ {
		val, exists := px.impl.doneMap[i]
		if !exists {
			val = -1
		}
		if val < minSeq {
			minSeq = val
		}
	}
	return minSeq
}

func (px *Paxos) getDoneVal() int{
	respChan := make (chan int)
	req := highestSeqDoneReq{
		responseChan: respChan,
	}
	px.impl.highestSeqDoneChan <- req
	val := <- respChan //receive the response
	return val
}

func (px *Paxos) alreadyDecided(seq int) bool{
	fate, _ := px.Status(seq)
	if(fate == Decided || fate == Forgotten){
		return true
	} 
	return false
}

func (px *Paxos) sendPeerInfo(peerID int, doneNum int) {
	peerDoneInfo := peerDoneReq{
		peerID:  peerID,
		doneNum: doneNum,
	}
	select {
	case px.impl.peerDoneChan <- peerDoneInfo:
	case <-px.term:
		return
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

func (px *Paxos) Start(seq int, v interface{}) {
	//sends the info to the goroutine that runs the paxos instance 
	// fmt.Println("Start RPC Called")
	req := startReq{
		seq: seq,
		value: v,
	}

	select {
	case px.impl.startChan <- req:
	case <-px.term:
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.

func (px *Paxos) Done(seq int) {
	req := doneReq{
		seq: seq,
	}

	select {
	case px.impl.doneChan <- req:
	case <-px.term:
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//

func (px *Paxos) Max() int {
	//iterate through map & return max response
	respChan := make(chan int)
	req := maxReq{
		responseChan: respChan,
	}

	select {
	case px.impl.maxChan <- req:
	case <-px.term:
		return 0
	}

	select {
	case reply := <-respChan:
		return reply
	case <-px.term:
		return 0
	}
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//

func (px *Paxos) Min() int {
	respChan := make(chan int)
	req := minReq{
		responseChan: respChan,
	}

	select {
	case px.impl.minChan <- req:
	case <-px.term:
		return 0
	}

	select {
	case minResp := <-respChan:
		return minResp
	case <-px.term:
		return 0
	}
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	respChan := make (chan Status)
	req := statusReq{
		seq: seq,
		responseChan: respChan,
	}

	select {
	case px.impl.statusChan <- req:
	case <-px.term:
		return Pending, nil
	}

	select {
	case statusReply := <-respChan:
		return statusReply.fate, statusReply.value
	case <-px.term:
		return Pending, nil
	}
}

/////////////////////////////////////////////////////////////////////////////

// RPC bodies


// Prepare (paxos phase one)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	
	respChan := make (chan PrepareReply)
	req := prepareReq{
		prepareArg: *args, //is this ok??
		responseChan: respChan,
	}

	select {
	case px.impl.prepareChan <- req:
	case <-px.term:
		return nil
	}

	px.sendPeerInfo(args.PeerID, args.Done)

	select{
	case prepReply := <- respChan:
		*reply = prepReply
		return nil
	case <- px.term:
		return nil
	}
}

// Accept (paxos phase two)
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	respChan := make (chan AcceptReply)
	req := acceptReq{
		acceptArg: *args,
		responseChan: respChan,
	}

	select {
	case px.impl.acceptChan <- req:
	case <-px.term:
		return nil
	}

	px.sendPeerInfo(args.PeerID, args.Done)

	select{
	case acceptResp := <- respChan:
		*reply = acceptResp
		return nil
	case <- px.term:
		return nil
	}
}

// Inform (the Decided optimization)
//not sure if we need an informReply

func (px *Paxos) Inform(args *InformArgs, reply *InformReply) error {
	respChan := make (chan InformReply)
	req := informReq{
		informArg: *args,
		responseChan: respChan,
	}
	
	select {
	case px.impl.informChan <- req:
	case <-px.term:
		return nil
	}

	px.sendPeerInfo(args.PeerID, args.Done)

	select{
	case informResp := <- respChan:
		*reply = informResp
		return nil
	case <- px.term:
		return nil
	}
}
