package logic

type ProposalID struct {
	number int
	uid    uint
}

/*
*Proposer type
 */
type Proposer struct {
	messanger    *Messanger
	proposer_uid uint
	quorum_size  int

	proposed_value    int
	proposal_id       *ProposalID
	last_accepted_id  int
	next_proposal_num int
	promises_rcvd     map[int]bool
}

func (proposer *Proposer) Init(messanger *Messanger) {
	proposer.next_proposal_num = 1
	proposer.promises_rcvd = make(map[int]bool)
	proposer.messanger = messanger
	messanger.RegProposer(proposer)
}

type Accepter struct {
	messanger      *Messanger
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
