package logic

type ProposalID struct {
	number int
	uid    uint
}

//True: this id is larger than the otherone
func (id *ProposalID) LargerThan(anotherID ProposalID) bool {
	if id.number > anotherID.number {
		return true
	}
	if id.number < anotherID.number {
		return false
	}
	return id.uid > anotherID.uid
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
	if prev_accepted_id.LargerThan(proposer.last_accepted_id) {
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

	promised_id    int
	accepted_id    int
	accepted_value int
}

func (accepter *Accepter) Init(messanger *Messanger) {
	accepter.messanger = messanger
	messanger.RegAccepter(accepter)
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
func (messanger *Messanger) send_prepare(proposal_id ProposalID) {

}

//Sends a Promise message to the specified Proposer
func (messanger *Messanger) send_promise(from_uid uint, proposal_id ProposalID, previous_id int, accepted_value int) {
}

//Broadcasts an Accept message to all Acceptors
func (messanger *Messanger) send_accept(proposal_id ProposalID, proposal_value int) {

}

//Broadcasts an Accepted message to all Learners
func (messanger *Messanger) send_accepted(proposal_id ProposalID, accepted_value int) {

}

//Called when a resolution is reached
func (messanger *Messanger) on_resolution(proposal_id ProposalID, value int) {

}
