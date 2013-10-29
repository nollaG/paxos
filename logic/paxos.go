package logic

import "fmt"
import "os"

type ProposalID struct {
	number int
	uid    uint
}

//>0: this id is larger than the otherone
//=0: equal
//<0: less
func (id *ProposalID) Compare(anotherID ProposalID) int {
	if id.number > anotherID.number {
		return 1
	}
	if id.number < anotherID.number {
		return -1
	}
	if id.uid > anotherID.uid {
		return 1
	}
	if id.uid < anotherID.uid {
		return -1
	}
	return 0
}

/*
*Proposer type
 */
type Proposer struct {
	name         string
	messanger    *Messanger
	proposer_uid uint
	quorum_size  int

	proposed_value    int
	proposal_id       ProposalID
	last_accepted_id  ProposalID
	next_proposal_num int
	promises_rcvd     map[uint]bool
}

func (proposer *Proposer) Init(messanger *Messanger, name string) {
	proposer.next_proposal_num = 1
	proposer.promises_rcvd = make(map[uint]bool)
	proposer.messanger = messanger
	proposer.name = name
	proposer.proposer_uid = messanger.RegProposer(proposer)
	proposer.proposed_value = -1
}

//Sets the proposal value for this node iff this node is not already aware of
//another proposal having already been accepted.
func (proposer *Proposer) set_proposal(value int) {
	if proposer.proposed_value != -1 {
		proposer.proposed_value = value
	}
}

//Sends a prepare request to all Acceptors as the first step in attempting to
//acquire leadership of the Paxos instance.
func (proposer *Proposer) prepare() {
	proposer.promises_rcvd = make(map[uint]bool)
	proposer.proposal_id = ProposalID{number: proposer.next_proposal_num, uid: proposer.proposer_uid}
	proposer.next_proposal_num += 1
	fmt.Printf(" [ %s ] send_prepare for id = %v\n", proposer.name, proposer.proposal_id)
	proposer.messanger.send_prepare(proposer.proposer_uid, proposer.proposal_id)
}

func (proposer *Proposer) recv_promise(from_uid uint, proposal_id ProposalID, prev_accepted_id ProposalID, prev_accepted_value int) {
	//Ignore the message if it's for an old proposal or we have already received a response from this Acceptor
	fmt.Printf(" [ %s ] recv_promise from %d with proposal_id = %v, prev_accepted_id = %v, prev_accepted_value = %d\n", proposer.name, from_uid, proposal_id, prev_accepted_id, prev_accepted_value)
	if proposal_id != proposer.proposal_id {
		return
	}
	if proposer.promises_rcvd[from_uid] {
		return
	}
	proposer.promises_rcvd[from_uid] = true
	if prev_accepted_id.Compare(proposer.last_accepted_id) > 0 {
		proposer.last_accepted_id = prev_accepted_id
		//If the Acceptor has already accepted a value, we MUST set our proposal to that value.
		if prev_accepted_value != -1 {
			proposer.proposed_value = prev_accepted_value
		}
	}
	proposer.quorum_size = len(proposer.messanger.Accepters)
	if len(proposer.promises_rcvd) == proposer.quorum_size {
		if proposer.proposed_value != -1 {
			fmt.Printf(" [ %s ] send_accept with id = %v, value = %d\n", proposer.name, proposer.proposal_id, proposer.proposed_value)
			proposer.messanger.send_accept(proposer.proposer_uid, proposer.proposal_id, proposer.proposed_value)
		}
	}
}

type Accepter struct {
	name         string
	messanger    *Messanger
	accepter_uid uint

	promised_id    ProposalID
	accepted_id    ProposalID
	accepted_value int
}

func (accepter *Accepter) Init(messanger *Messanger, name string) {
	accepter.messanger = messanger
	accepter.name = name
	accepter.accepter_uid = messanger.RegAccepter(accepter)
	accepter.accepted_value = -1
}

//Called when a Prepare message is received from a Proposer
func (accepter *Accepter) recv_prepare(from_uid uint, proposal_id ProposalID) {
	fmt.Printf(" [ %s ] recv_prepare from %d with proposal_id = %v\n", accepter.name, from_uid, proposal_id)
	//Duplicate prepare message
	if proposal_id == accepter.promised_id {
		accepter.messanger.send_promise(from_uid, proposal_id, accepter.accepted_id, accepter.accepted_value)
	} else if proposal_id.Compare(accepter.promised_id) > 0 {
		accepter.promised_id = proposal_id
		fmt.Printf(" [ %s ] send_promise to %d with proposal_id = %v, accepted_id = %v, accepted_value = %d\n", accepter.name, from_uid, proposal_id, accepter.accepted_id, accepter.accepted_value)
		accepter.messanger.send_promise(from_uid, proposal_id, accepter.accepted_id, accepter.accepted_value)
	}
}

//Called when an Accept message is received from a Proposer
func (accepter *Accepter) recv_accept_request(from_uid uint, proposal_id ProposalID, value int) {
	fmt.Printf(" [ %s ] recv_accept_request from %d with proposal_id = %v, value = %d\n", accepter.name, from_uid, proposal_id, value)
	if proposal_id.Compare(accepter.promised_id) >= 0 {
		accepter.promised_id = proposal_id
		accepter.accepted_id = proposal_id
		accepter.accepted_value = value
		fmt.Printf(" [ %s ] send_accepted with proposal_id = %v, value = %d\n", accepter.name, proposal_id, value)
		accepter.messanger.send_accepted(accepter.accepter_uid, proposal_id, value)
	}
}

type Learner struct {
	name        string
	quorum_size int
	learner_uid uint
	messanger   *Messanger

	proposals         map[ProposalID][]int //maps proposal_id => [accept_count, retain_count, value]
	accepters         map[uint]ProposalID  //maps from_uid => last_accepted_proposal_id
	final_value       int
	final_proposal_id ProposalID
}

func (learner *Learner) Init(messanger *Messanger, name string) {
	learner.proposals = make(map[ProposalID][]int)
	learner.accepters = make(map[uint]ProposalID)
	learner.final_value = -1
	learner.messanger = messanger
	learner.name = name
	learner.learner_uid = messanger.RegLearner(learner)
}

func (learner *Learner) isComplete() bool {
	return learner.final_value != -1
}

//Called when an Accepted message is received from an acceptor
func (learner *Learner) recv_accepted(from_uid uint, proposal_id ProposalID, accepted_value int) {
	fmt.Printf(" [ %s ] recv_accepted from %d with proposal_id %v, accepted_value = %d\n", learner.name, from_uid, proposal_id, accepted_value)
	if learner.final_value != -1 {
		return //already complete
	}
	last_pn, ok := learner.accepters[from_uid]
	if proposal_id.Compare(last_pn) <= 0 {
		return
	}
	learner.accepters[from_uid] = proposal_id
	if ok {
		oldp := learner.proposals[last_pn]
		oldp[1] -= 1
		if oldp[1] == 0 {
			delete(learner.proposals, last_pn)
		}
	}
	if _, ok = learner.proposals[proposal_id]; !ok {
		learner.proposals[proposal_id] = make([]int, 3)
		learner.proposals[proposal_id][0] = 0
		learner.proposals[proposal_id][1] = 0
		learner.proposals[proposal_id][2] = accepted_value

	}
	t := learner.proposals[proposal_id]
	t[0]++
	t[1]++
	learner.quorum_size = len(learner.messanger.Accepters)
	if t[0] == learner.quorum_size {
		learner.final_value = accepted_value
		learner.final_proposal_id = proposal_id
		learner.proposals = make(map[ProposalID][]int)
		learner.accepters = make(map[uint]ProposalID)
		learner.messanger.on_resolution(proposal_id, accepted_value)
	}

}

type Messanger struct {
	Proposers []*Proposer
	Accepters []*Accepter
	Learners  []*Learner
	next_uid  uint
}

func (messanger *Messanger) Init() {
	messanger.Proposers = make([]*Proposer, 0)
	messanger.Accepters = make([]*Accepter, 0)
	messanger.Learners = make([]*Learner, 0)
	messanger.next_uid = 0
}

func (messanger *Messanger) RegProposer(proposer *Proposer) (uid uint) {
	messanger.Proposers = append(messanger.Proposers, proposer)
	messanger.next_uid++
	return messanger.next_uid
}
func (messanger *Messanger) RegAccepter(accepter *Accepter) (uid uint) {
	messanger.Accepters = append(messanger.Accepters, accepter)
	messanger.next_uid++
	return messanger.next_uid
}
func (messanger *Messanger) RegLearner(learner *Learner) (uid uint) {
	messanger.Learners = append(messanger.Learners, learner)
	messanger.next_uid++
	return messanger.next_uid
}

//Broadcasts a Prepare message to all Acceptors
//from_uid indicates the proposer id
func (messanger *Messanger) send_prepare(from_uid uint, proposal_id ProposalID) {
	for i := 0; i < len(messanger.Accepters); i++ {
		go messanger.Accepters[i].recv_prepare(from_uid, proposal_id)
	}
}

//Sends a Promise message to the specified Proposer
//to_uid indicates the proposer id
func (messanger *Messanger) send_promise(to_uid uint, proposal_id ProposalID, previous_id ProposalID, accepted_value int) {
	for i := 0; i < len(messanger.Proposers); i++ {
		if messanger.Proposers[i].proposer_uid == to_uid {
			go messanger.Proposers[i].recv_promise(to_uid, proposal_id, previous_id, accepted_value)
			return
		}
	}
}

//Broadcasts an Accept message to all Acceptors
//from_uid indicates the proposer id
func (messanger *Messanger) send_accept(from_uid uint, proposal_id ProposalID, proposal_value int) {
	for i := 0; i < len(messanger.Accepters); i++ {
		go messanger.Accepters[i].recv_accept_request(from_uid, proposal_id, proposal_value)
	}

}

//Broadcasts an Accepted message to all Learners
//from_uid indicates the accepter id
func (messanger *Messanger) send_accepted(from_uid uint, proposal_id ProposalID, accepted_value int) {
	for i := 0; i < len(messanger.Learners); i++ {
		go messanger.Learners[i].recv_accepted(from_uid, proposal_id, accepted_value)
	}

}

//Called when a resolution is reached
func (messanger *Messanger) on_resolution(proposal_id ProposalID, value int) {
	fmt.Println(" [ Resolution ] with proposal_id = %v, value = %d", proposal_id, value)
	os.Exit(0)
}
