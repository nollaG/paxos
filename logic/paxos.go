package logic

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
	messanger    *Messanger
	proposer_uid uint
	quorum_size  int

	proposed_value    int
	proposal_id       ProposalID
	last_accepted_id  ProposalID
	next_proposal_num int
	promises_rcvd     map[uint]bool
}

func (proposer *Proposer) Init(messanger *Messanger) {
	proposer.next_proposal_num = 1
	proposer.promises_rcvd = make(map[uint]bool)
	proposer.messanger = messanger
	messanger.RegProposer(proposer)
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
	proposer.messanger.send_prepare(proposer.proposal_id)
}

func (proposer *Proposer) recv_promise(from_uid uint, proposal_id ProposalID, prev_accepted_id ProposalID, prev_accepted_value int) {
	//Ignore the message if it's for an old proposal or we have already received a response from this Acceptor
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
	if len(proposer.promises_rcvd) == proposer.quorum_size {
		if proposer.proposed_value != -1 {
			proposer.messanger.send_accept(proposer.proposal_id, proposer.proposed_value)
		}
	}
}

type Accepter struct {
	messanger   *Messanger
	accepter_id uint

	promised_id    ProposalID
	accepted_id    ProposalID
	accepted_value int
}

func (accepter *Accepter) Init(messanger *Messanger) {
	accepter.messanger = messanger
	messanger.RegAccepter(accepter)
	accepter.accepted_value = -1
}

//Called when a Prepare message is received from a Proposer
func (accepter *Accepter) recv_prepare(from_uid uint, proposal_id ProposalID) {
	//Duplicate prepare message
	if proposal_id == accepter.promised_id {
		accepter.messanger.send_promise(from_uid, proposal_id, accepter.accepted_id, accepter.accepted_value)
	} else if proposal_id.Compare(accepter.promised_id) > 0 {
		accepter.promised_id = proposal_id
		accepter.messanger.send_promise(from_uid, proposal_id, accepter.accepted_id, accepter.accepted_value)
	}
}

//Called when an Accept message is received from a Proposer
func (accepter *Accepter) recv_accept_request(from_uid uint, proposal_id ProposalID, value int) {
	if proposal_id.Compare(accepter.promised_id) >= 0 {
		accepter.promised_id = proposal_id
		accepter.accepted_id = proposal_id
		accepter.accepted_value = value
		accepter.messanger.send_accepted(accepter.accepter_id, proposal_id, value)
	}
}

type Learner struct {
	quorum_size int

	proposals         map[int][]int //maps proposal_id => [accept_count, retain_count, value]
	accepters         map[uint]int  //maps from_uid => last_accepted_proposal_id
	final_value       int
	final_proposal_id int
}

func (learner *Learner) Init(messanger *Messanger) {
	learner.proposals = make(map[int][]int)
	learner.accepters = make(map[uint]int)
	messanger.RegLearner(learner)
}

type Messanger struct {
	Proposers []*Proposer
	Accepters []*Accepter
	Learners  []*Learner
}

func (messanger *Messanger) Init() {
	messanger.Proposers = make([]*Proposer, 0)
	messanger.Accepters = make([]*Accepter, 0)
	messanger.Learners = make([]*Learner, 0)
}

func (messanger *Messanger) RegProposer(proposer *Proposer) {
	messanger.Proposers = append(messanger.Proposers, proposer)
}
func (messanger *Messanger) RegAccepter(accepter *Accepter) {
	messanger.Accepters = append(messanger.Accepters, accepter)
}
func (messanger *Messanger) RegLearner(learner *Learner) {
	messanger.Learners = append(messanger.Learners, learner)
}

//Broadcasts a Prepare message to all Acceptors
//from_uid indicates the proposer id
func (messanger *Messanger) send_prepare(from_uid uint, proposal_id ProposalID) {

}

//Sends a Promise message to the specified Proposer
//to_uid indicates the proposer id
func (messanger *Messanger) send_promise(to_uid uint, proposal_id ProposalID, previous_id ProposalID, accepted_value int) {
}

//Broadcasts an Accept message to all Acceptors
//from_uid indicates the proposer id
func (messanger *Messanger) send_accept(from_uid uint, proposal_id ProposalID, proposal_value int) {

}

//Broadcasts an Accepted message to all Learners
//from_uid indicates the accepter id
func (messanger *Messanger) send_accepted(from_uid uint, proposal_id ProposalID, accepted_value int) {

}

//Called when a resolution is reached
func (messanger *Messanger) on_resolution(proposal_id ProposalID, value int) {

}
