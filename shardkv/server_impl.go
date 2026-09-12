package shardkv

import (
	"fmt"
	"time"

	"umich.edu/eecs491/proj4/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//

type Op struct {
	Key string
	Value string //value for put/append
	OpID int64
	ClientID int64 //used to detect duplicate client requests
	OpType string
	Ai assignInfo
	Pa PullArgs //tells us what to pull from our own thing
	PaxosID int64
}

//request types
type getReq struct{
	arg GetArgs
	responseChan chan(GetReply)
}

type putAppendReq struct{
	arg PutAppendArgs
	responseChan chan(PutAppendReply)
}

type pullReq struct{
	arg PullArgs
	responseChan chan(PullReply)
}

type assignReq struct{
	arg common.AssignArgs
	responseChan chan(common.AssignReply)
}

type assignInfo struct{
	ConfigNum int
	Err Err
	Shard int
	KVStore map[string]string //map shard number -> shard
	OpCache map[int64]int64
}

type Shard struct{
	kVStore map[string]string //map shard number -> shard
	opCache map[int64]int64
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	//compare assign args ==> update it here
	op1, ok1 := v1.(Op)
	op2, ok2 := v2.(Op)

	if(!ok1 || !ok2){
		fmt.Println("okkk something is seriously wrong here")
	}
	if(op1.PaxosID == op2.PaxosID){
		return true
	}
	return false
}

//
// additions to ShardKV state
//
type ShardKVImpl struct { //this is per shard (duplicated across all servers in the shard)
	getChan chan getReq
	putAppendChan chan putAppendReq
	pullChan chan pullReq
	assignChan chan assignReq

	shardMap map[int]Shard
	oldShardMap map[int]Shard
	latestConfig int
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
	kv.impl.getChan = make(chan getReq, 1000)
	kv.impl.putAppendChan = make(chan putAppendReq, 1000)
	kv.impl.pullChan = make(chan pullReq, 1000)
	kv.impl.assignChan = make(chan assignReq, 1000)

	//add in the shard map here
	kv.impl.shardMap = make(map[int]Shard)
	kv.impl.oldShardMap = make(map[int]Shard)

	kv.impl.latestConfig = 0
	go kv.eventLoop();
}

func (kv *ShardKV) eventLoop(){
	for{
		select{
		case <- kv.term:
			return
		case getReq := <- kv.impl.getChan:
			newOp := Op{
				Key: getReq.arg.Key,
				Value: "",
				OpID: getReq.arg.OpID,
				ClientID: getReq.arg.ClientID,
				OpType: "Get",
				PaxosID: common.Nrand(),
			}

			//send the new op to the channel
			kv.rsm.AddOp(newOp, equals)

			var resErr Err
			var val string

			shardNum := common.Key2Shard(getReq.arg.Key)
			shardInst, exists := kv.impl.shardMap[shardNum]

			if(!exists){
				resErr = ErrWrongGroup
			} else{
				shardVal, ok := shardInst.kVStore[getReq.arg.Key]
				if(!ok){
					fmt.Println("get sending error no key")
					resErr = ErrNoKey
				} else{
					val = shardVal
					resErr = OK
				}
			}

			r := GetReply{
				Value: val,
				Err: resErr,
			}
			getReq.responseChan <- r

		case putAppendReq := <- kv.impl.putAppendChan:
			var opType string

			if(putAppendReq.arg.Op == "Put"){
				opType = "Put"
			} else{
				opType = "Append"
			}

			newOp := Op{
				Key: putAppendReq.arg.Key,
				Value: putAppendReq.arg.Value,
				OpID: putAppendReq.arg.OpID,
				ClientID: putAppendReq.arg.ClientID,
				OpType: opType,
				PaxosID: common.Nrand(),
			}
			
			kv.rsm.AddOp(newOp, equals)

			var resErr Err

			//now we know our local state is completely updated
			shardNum := common.Key2Shard(putAppendReq.arg.Key)
			_, exists := kv.impl.shardMap[shardNum]

			if(!exists){ //should only check our current shard map, 
				//if we used to own the key we shouldn't change local state
				fmt.Println("put append returning err wrong group, gid ", kv.gid, "shard", shardNum)
				resErr = ErrWrongGroup
			} else{
				resErr = OK
			}

			r := PutAppendReply{
				Err: resErr,
			}

			putAppendReq.responseChan <- r

		case assignReq := <- kv.impl.assignChan:
			fmt.Println("in event loop, gid", kv.gid, "getting an assign for shard", assignReq.arg.Shard)

			var RPCReply PullReply

			if(assignReq.arg.ConfigNum != 1){
				RPCReply = kv.PullRPCCalls(assignReq.arg)
				if(RPCReply.Err != OK){
					ar := common.AssignReply{}
					assignReq.responseChan <- ar
					continue //done w/ this, no need to add to Paxos
				}
			}
			
			var ai assignInfo
			if(assignReq.arg.ConfigNum == 1){
				fmt.Println("assignArgs.ConfigNum == 1")
				//create a blank shard map (fake pull reply)
				ai = assignInfo{
					Err: OK,
					ConfigNum: 1,
					Shard: assignReq.arg.Shard,
					KVStore: make(map[string]string),
					OpCache: make(map[int64]int64),
				}
			} else{
				ai = assignInfo{
					Err: OK,
					ConfigNum: assignReq.arg.ConfigNum,
					Shard: assignReq.arg.Shard,
					KVStore: copyKV(RPCReply.KVStore),
					OpCache: copyCache(RPCReply.OpIDCache),
				}
			}

			newOp := Op{
				OpType: "Assign",
				Ai: ai,
				PaxosID: common.Nrand(),
			}

			kv.rsm.AddOp(newOp, equals)

			ar := common.AssignReply{}
			assignReq.responseChan <- ar

		case pullReq := <- kv.impl.pullChan:
			fmt.Println("in event loop, got a pull req in EL for shard ", pullReq.arg.Shard)

			//add to Paxos log:
			newOp := Op{
				OpType: "Pull", 
				Pa: pullReq.arg,
				PaxosID: common.Nrand(),
			}

			//keep track of the latest config we have assigned
			//keep track of client
			if(pullReq.arg.ConfigNum < kv.impl.latestConfig){ //outdated config
				//we don't want to handle this pull request, is old
				pr := PullReply{
					Err: ErrOld,
					KVStore: map[string]string{},
					OpIDCache: map[int64]int64{},
				}
				pullReq.responseChan <- pr
				break
			}

			//send a snapshot request to the addOpChan
			kv.rsm.AddOp(newOp, equals)

			//now that we are caught up, check again if its an old request
			if(pullReq.arg.ConfigNum < kv.impl.latestConfig){ //outdated config
				//we don't want to handle this pull request, is already a duplicate value
				fmt.Println("pull gonna return an error old here")
				pr := PullReply{
					Err: ErrOld,
					KVStore: make(map[string]string),
					OpIDCache: make(map[int64]int64),
				}
				pullReq.responseChan <- pr
				continue
			}

			shardNum := pullReq.arg.Shard
			inst2, exists2 := kv.impl.oldShardMap[shardNum]

			//once we've run pull, it is guarenteed to be in our old map
			var pr PullReply
			if(exists2){
				pr = PullReply{
					Err: OK,
					KVStore: copyKV(inst2.kVStore),
					OpIDCache: copyCache(inst2.opCache),
				}
			} else{
				pr = PullReply{
					Err: ErrWrongGroup,
					KVStore: make(map[string]string),
					OpIDCache: make(map[int64]int64),
				}
			}
			//send the response into the channel
			pullReq.responseChan <- pr
		}
	}
}

func (kv *ShardKV) PullRPCCalls(args common.AssignArgs) PullReply{
	//assignArgs retry request
	for{
		for i := 0; i < len(args.Servers); i++{
			//ping the servers, send a pull RPC call
			pa := PullArgs{
				ConfigNum: args.ConfigNum, 
				Shard: args.Shard,
			}
			var reply PullReply
			ok := common.Call(args.Servers[i], "ShardKV.PullShard", &pa, &reply)
			if(ok && reply.Err == OK){
				fmt.Println("found = true")
				return reply
			} else if(ok && (reply.Err == ErrOld)){
				return reply
			} else if(reply.Err == ErrWrongGroup){
				fmt.Println("hmm returning error wrong group here")
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

//functions here
func copyKV(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyCache(src map[int64]int64) map[int64]int64 {
	if src == nil {
		return make(map[int64]int64)
	}
	dst := make(map[int64]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

	respChan := make (chan GetReply)
	req := getReq{
		arg: *args,
		responseChan: respChan,
	}

	select {
	case kv.impl.getChan <- req:
	case <- kv.term:
		return nil
	}

	select{
	case getResp := <- respChan:
		*reply = getResp
		return nil
	case <- kv.term:
		return nil
	}
}

//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	respChan := make (chan PutAppendReply)
	req := putAppendReq{
		arg: *args,
		responseChan: respChan,
	}

	select {
	case kv.impl.putAppendChan <- req:
	case <- kv.term:
		return nil
	}

	select{
	case resp := <- respChan:
		*reply = resp
		return nil
	case <- kv.term:
		return nil
	}
}

func (kv *ShardKV) checkIfDuplicate(shard int, opID int64, clientID int64) bool{
	inst, ok1 := kv.impl.shardMap[shard]
	if(ok1){
		instID, ok := inst.opCache[clientID]
		if(ok && instID >= opID){
			fmt.Println("returning upon cache hit, alr did this op")
			return true
		}
	}
	return false
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
	op := v.(Op)
	fmt.Println("applying", op.OpType, " on client", op.ClientID, " with opid ", op.OpID)

	if op.OpType == "Put" || op.OpType == "Get" || op.OpType == "Append"{
		shardNum := common.Key2Shard(op.Key)
		inst, exists := kv.impl.shardMap[shardNum]
		if(exists){
			if(kv.checkIfDuplicate(shardNum, op.OpID, op.ClientID)){
				return
			}
			//update cache
			inst.opCache[op.ClientID] = op.OpID

			if(op.OpType == "Put"){
				inst.kVStore[op.Key] = op.Value
			}
			if(op.OpType == "Append"){
				inst.kVStore[op.Key] += op.Value
			}
			kv.impl.shardMap[shardNum] = inst
		}
	} else if(op.OpType == "Assign"){
		assignInfo := op.Ai
		shardNum := assignInfo.Shard

		//could assign here ever be < our latest config? something to check
		//now we have our pullReply; update our kvstore for this shard
		if(assignInfo.Err == OK){
			fmt.Println("shard we're receiving (in assign): ", shardNum, "gid: ", kv.gid)
			//pull this shard here
			shard := Shard{}
			shard.kVStore = copyKV(assignInfo.KVStore)
			shard.opCache = copyCache(assignInfo.OpCache)
			kv.impl.shardMap[shardNum] = shard

			//mutate local state
			if(assignInfo.ConfigNum > kv.impl.latestConfig){
				fmt.Println("changing config num in assign")
				kv.impl.latestConfig = assignInfo.ConfigNum
			}
		}
	}

	if(op.OpType == "Pull"){ //if we get a pull request
		fmt.Println("handling pull operation in applyOp, gid: ", kv.gid)
		fmt.Println("our latest config: ", kv.impl.latestConfig)
		fmt.Println("pull args config: ", op.Pa.ConfigNum)

		if(op.Pa.ConfigNum < kv.impl.latestConfig){
			fmt.Println("in pull, our config numbers are not aligned")
			return
			//don't do anything
		}

		shardNum := op.Pa.Shard
		fmt.Println("shard we're giving away (in pull): ", shardNum, "gid: ", kv.gid)

		//delete this shard from our map, we gave it to someone else
		_, exists := kv.impl.shardMap[shardNum]

		if exists {
			//update the stuff in our old shard in case we need it
			old := kv.impl.shardMap[shardNum]
			newShard := Shard{
				kVStore: copyKV(old.kVStore),
				opCache: copyCache(old.opCache),
			}
			kv.impl.oldShardMap[shardNum] = newShard
			delete(kv.impl.shardMap, shardNum)
		}

		fmt.Println("exiting pull")
	}
}

//
// Assign a shard to this group. Called by ShardMaster
//
func (kv *ShardKV) AssignShard(args *common.AssignArgs, reply *common.AssignReply) error {
	fmt.Println("in assign shard RPC handler")

	respChan := make (chan common.AssignReply)
	req := assignReq{
		arg: *args,
		responseChan: respChan,
	}

	select {
	case kv.impl.assignChan <- req:
	case <- kv.term:
		return nil
	}

	select{
	case resp := <- respChan:
		*reply = resp
		return nil
	case <- kv.term:
		return nil
	}
}

//
// Pull a shard from this group. Called by another shardkv server
//

func (kv *ShardKV) PullShard(args *PullArgs, reply *PullReply) error {
	//when we get pull shard, we log in the Paxos store & remove from our local K/V store
	//thus, after this we won't get any requests / return error wrong server
	//pullShard only returns AFTER this has already been added to the shard's paxos log
	respChan := make (chan PullReply)
	req := pullReq{
		arg: *args,
		responseChan: respChan,
	}

	select {
	case kv.impl.pullChan <- req:
	case <- kv.term:
		return nil
	}

	select{
	case resp := <- respChan:
		*reply = resp
		return nil
	case <- kv.term:
		return nil
	}
}