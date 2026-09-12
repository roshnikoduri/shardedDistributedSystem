package shardmaster

import "fmt"
import "sort"
import "umich.edu/eecs491/proj4/common"

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//

//using the map method for delegating join/leave shards
type serverShards struct {
    Server int
    Shards []int64
}

type Op struct {
	Args OpArgs
	OpType string
	OpID int64
}

//the op takes in 
type OpArgs struct{
	Ja JoinArgs 
	La LeaveArgs
	Ma MoveArgs
	Qa QueryArgs
}

type JoinRequest struct{
	args JoinArgs
	respChan chan(JoinReply)
}

type LeaveRequest struct{
	args LeaveArgs
	respChan chan(LeaveReply)
}

type MoveRequest struct{
	args MoveArgs
	respChan chan(MoveReply)
}

type QueryRequest struct{
	args QueryArgs
	respChan chan(QueryReply)
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1, ok1 := v1.(Op)
	op2, ok2 := v2.(Op)

	if(!ok1 || !ok2){
		fmt.Println("idk panic theres some assertion issues")
		return false
	}

	if(op1.OpID == op2.OpID){
		return true
	}
	
	return false
}

//
// additions to ShardMaster state
//
type ShardMasterImpl struct {
	configMap map[int]Config //matches config number to the actual config

	joinChan chan(JoinRequest)
	leaveChan chan(LeaveRequest)
	moveChan chan(MoveRequest)
	queryChan chan(QueryRequest)

	latestConfig int
	curNumGroups int
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
	sm.impl.configMap = make(map[int]Config)

	sm.impl.joinChan = make(chan JoinRequest, 1000)
	sm.impl.leaveChan = make(chan LeaveRequest, 1000)
	sm.impl.moveChan = make(chan MoveRequest, 1000)
	sm.impl.queryChan = make(chan QueryRequest, 1000)

	sm.impl.latestConfig = 0
	sm.impl.curNumGroups = 0

	var firstShards [16]int64
	for i:= 0; i < common.NShards; i++{
		firstShards[i] = 0
	}
	var firstGroups map[int64][]string 

	firstConfig := Config{
		Num: 0,
		Shards: firstShards,
		Groups: firstGroups,
	}

	sm.impl.configMap[0] = firstConfig
	go sm.eventLoop()
}

func (sm *ShardMaster) eventLoop(){
	for{
		select{
		case <- sm.term:
			return
		
		case joinReq := <- sm.impl.joinChan:
			//construct a join request & ask the RSM layer to agree on the join req
			newOp := Op{
				OpType: "Join",
				Args: OpArgs{
					Ja: joinReq.args,
				},
				OpID: common.Nrand(),
			}

			//wait for the new operation to be applied (this implicitly calls
			//our applyOp as well)
			sm.rsm.AddOp(newOp, equals)

			sm.makeRPCCall()
			//send the response to the client
			r := JoinReply{}
			joinReq.respChan <- r
		
		case leaveReq := <- sm.impl.leaveChan:

			newOp := Op{
				OpType: "Leave",
				Args: OpArgs{
					La: leaveReq.args,
				},
				OpID: common.Nrand(),
			}
			
			sm.rsm.AddOp(newOp, equals)

			sm.makeRPCCall()
			//send the response to the client
			r := LeaveReply{}
			leaveReq.respChan <- r

		case moveReq := <- sm.impl.moveChan:
			newOp := Op{
				OpType: "Move",
				Args: OpArgs{
					Ma: moveReq.args,
				},
				OpID: common.Nrand(),
			}
			sm.rsm.AddOp(newOp, equals)
			sm.makeRPCCall()
			
			//send the response to the client
			r := MoveReply{}
			moveReq.respChan <- r
		
		case queryReq := <- sm.impl.queryChan:
			newOp := Op{
				OpType: "Query",
				Args: OpArgs{
					Qa: queryReq.args,
				},
				OpID: common.Nrand(),
			}

			//this just adds to the log but doesn't mutate state
			sm.rsm.AddOp(newOp, equals)

			//send the requested config to the client
			fmt.Println("query req num: ", queryReq.args.Num)

			reqNum := queryReq.args.Num
			if(queryReq.args.Num == -1 || queryReq.args.Num > sm.impl.latestConfig){
				reqNum = sm.impl.latestConfig
			}

			r := QueryReply{sm.impl.configMap[reqNum]}
			queryReq.respChan <- r
		}
	}
}

func (sm *ShardMaster) makeRPCCall(){
	//send out the RPC calls to compare the configs
	//compare each of the configs
	oldConfig := sm.impl.configMap[sm.impl.latestConfig - 1]
	curConfig := sm.impl.configMap[sm.impl.latestConfig]
	fmt.Println("making RPC call, with config: ", sm.impl.latestConfig)

	//go through each of the shards
	for i := 0; i < len(curConfig.Shards); i++{
		if oldConfig.Shards[i] != curConfig.Shards[i]{
			fmt.Println("shardmaster reassigning shard ", i)
			//go through every possible server
			oldGid := oldConfig.Shards[i]
			newGid := curConfig.Shards[i]
			found := false
			for !found{
				for j := 0; j < len(curConfig.Groups[newGid]); j++{ //go through groups in the new config
					var reply common.AssignReply
					args := common.AssignArgs{
						ConfigNum: sm.impl.latestConfig,
						Shard: i,
						Servers: oldConfig.Groups[oldGid],
					}
					server := curConfig.Groups[newGid][j]
					fmt.Println("about to call server: ", server)
					ok := common.Call(server, "ShardKV.AssignShard", &args, &reply)
					if(ok){
						fmt.Println("Shardmaster breakout out of the loop here")
						found = true
						break
					}else{
						fmt.Println("RPC call failed unforch")
					}
				}
			}
		}
	}
}

//
// RPC handlers for Join, Leave, Move, and Query RPCs
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	fmt.Println("~~~~~~~in join RPC call~~~~~~~")

	respChan := make (chan JoinReply)
	req := JoinRequest{
		args: *args,
		respChan: respChan,
	}

	select {
	case sm.impl.joinChan <- req:
	case <- sm.term:
		return nil
	}

	select{
	case joinResp := <- respChan:
		*reply = joinResp
		return nil
	case <- sm.term:
		return nil
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	fmt.Println("~~~~~~~~~in leave RPC call~~~~~~~~")

	respChan := make (chan LeaveReply)
	req := LeaveRequest{
		args: *args,
		respChan: respChan,
	}

	select {
	case sm.impl.leaveChan <- req:
	case <- sm.term:
		return nil
	}

	select{
	case leaveResp := <- respChan:
		*reply = leaveResp
		return nil
	case <- sm.term:
		return nil
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	fmt.Println("~~~~~~~~~~in move RPC call~~~~~~~~~~")

	respChan := make (chan MoveReply)
	req := MoveRequest{
		args: *args,
		respChan: respChan,
	}

	select {
	case sm.impl.moveChan <- req:
	case <- sm.term:
		return nil
	}

	select{
	case moveResp := <- respChan:
		*reply = moveResp
		return nil
	case <- sm.term:
		return nil
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {

	respChan := make (chan QueryReply)
	req := QueryRequest{
		args: *args,
		respChan: respChan,
	}

	select {
	case sm.impl.queryChan <- req:
	case <- sm.term:
		return nil
	}

	select{
	case queryResp := <- respChan:
		*reply = queryResp
		return nil
	case <- sm.term:
		return nil
	}
}

func sortByDescShardCount(list []serverShards) {
    sort.Slice(list, func(i, j int) bool {
        // Primary: descending shard count
        if len(list[i].Shards) != len(list[j].Shards) {
            return len(list[i].Shards) > len(list[j].Shards)
        }
        // Tie-breaker: descending GroupID
        return list[i].Server > list[j].Server
    })
}

//rebalance function
func (sm *ShardMaster) balanceShards(targetGID int64, isLeave bool) [16]int64{
	//if leaving, add everything to the pool of available shards
	//if joining, pool of available shards = 0

	var newShards[16] int64

	//if this is the first join, give everything to target shard and return
	if(sm.impl.latestConfig == 0){
		fmt.Println("in shardmaster initial if statement")
		fmt.Println("initial new server: ", targetGID)

		if(isLeave){
			fmt.Println("panic idk why this is getting called on a leave")
		}
		for i := 0; i < common.NShards; i++{
			newShards[i] = targetGID
		}
		return newShards
	}

	curConfig := sm.impl.configMap[sm.impl.latestConfig]

	//check to see if it already exists here in the map
	_, exists := curConfig.Groups[targetGID]
	if(!isLeave && exists){
		//we've already joined
		newShards = curConfig.Shards
		return newShards
	}
	
	//make a map from this current config
	serversToShards := make(map[int64][]int64)
	
	//go through current state of shards
	for i := 0; i < common.NShards; i++{
		serversToShards[curConfig.Shards[i]] = append(serversToShards[curConfig.Shards[i]], int64(i))
	}
	
	if(!isLeave){ //if its a join, add this to the list
		serversToShards[targetGID] = []int64{}
	}

	var availablePool []int64 //available shards
	if(isLeave){ //leave starts the available pool
		availablePool = serversToShards[targetGID]
	}

	//sort the map
	list := []serverShards{}
	for s, shards := range serversToShards {
		if(isLeave && s == targetGID){ //for leave, we have already added this server's shards
			continue
		}
		list = append(list, serverShards{Server: int(s), Shards: shards})
	}
	for key := range curConfig.Groups { //make sure any groups w/ no shards are still in the list
		_, exists = serversToShards[key]
		if(!exists){
			list = append(list, serverShards{Server: int(key), Shards: []int64{}})
		}
	}

	sortByDescShardCount(list)

	//calculate rebalancing metrics
	numGroups := len(list)
	if(numGroups == 0){
		newShards = curConfig.Shards
		return newShards
	}
	
	base := common.NShards / numGroups
	extra := common.NShards % numGroups

	//go through the list & figure out target stuff
	for i := 0; i < len(list); i++{
		targetNum := base
		if(i < extra){
			targetNum = base+1
		}

		if(len(list[i].Shards) == targetNum){
			continue
		}

		for len(list[i].Shards) > targetNum{
			//add to the available pool
			availablePool = append(availablePool, list[i].Shards[len(list[i].Shards) - 1])
			list[i].Shards = list[i].Shards[:len(list[i].Shards)-1]
		}

		for len(list[i].Shards) < targetNum{
			//take from the available pool
			list[i].Shards = append(list[i].Shards, availablePool[len(availablePool) - 1])
			availablePool = availablePool[:len(availablePool)-1]
		}
	}

	//convert back to a regular array (shards)
	for key := range(list){
		for i:= 0; i < len(list[key].Shards); i++{
			newShards[list[key].Shards[i]] = int64(list[key].Server)
		}
	}

	return newShards
}

//map ==> servers to shards
func (sm *ShardMaster) createNewConfig(shards [16]int64){
	curConfig := sm.impl.configMap[sm.impl.latestConfig]
	newConfigNum := sm.impl.latestConfig + 1
	//just copy the groups from the past configs, update it later in RPC handlers
	newGroups := make(map[int64][]string)
	for gid, servers := range curConfig.Groups {
		copiedServers := make([]string, len(servers))
		copy(copiedServers, servers)
		newGroups[gid] = copiedServers
	}
	newShards := shards
	newConfig := Config{
		Num: newConfigNum,
		Shards: newShards,
		Groups: newGroups,
	}
	sm.impl.latestConfig = newConfigNum
	sm.impl.configMap[sm.impl.latestConfig] = newConfig
}

func (sm *ShardMaster) applyMove(targetShard int64, targetGroup int64) [16]int64{
	//change the map serverToShards
	//change the global shards map
	curConfig := sm.impl.configMap[sm.impl.latestConfig]
	var newShards [16]int64
	newShards = curConfig.Shards
	newShards[int(targetShard)] = targetGroup
	return newShards
}

//
// Execute operation encoded in decided value v and update local state
//
func (sm *ShardMaster) ApplyOp(v interface{}) {
	op, ok := v.(Op)
	
	if(!ok){
		fmt.Println("smtn is seriously wrong here")
		return
	}
	fmt.Println("inside apply op in shardmaster")

	//otherwise, look at the op
	if(op.OpType == "Join"){
		//call the rebalance function
		gid := op.Args.Ja.GID
		newShards := sm.balanceShards(gid, false)
		sm.createNewConfig(newShards)
		//add new group to the map
		sm.impl.configMap[sm.impl.latestConfig].Groups[gid] = op.Args.Ja.Servers
	} else if(op.OpType == "Leave"){
		//go one-by-one and give shards to everyone
		gid := op.Args.La.GID
		newShards := sm.balanceShards(gid, true)
		sm.createNewConfig(newShards)
		delete(sm.impl.configMap[sm.impl.latestConfig].Groups, gid)
	} else if(op.OpType == "Move"){
		newShards := sm.applyMove(int64(op.Args.Ma.Shard), op.Args.Ma.GID)
		sm.createNewConfig(newShards)
	}

	if(op.OpType == "Query"){
		return
	}

	return
}


