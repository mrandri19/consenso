from src.single_decree_paxos import (
    AcceptCallback,
    Acceptor,
    AcceptorId,
    AcceptRequest,
    AcceptResponse,
    PrepareCallback,
    PrepareRequest,
    PrepareResponse,
    ProposalNumber,
    Proposer,
    Transport,
    Value,
)


def test_proposal_number() -> None:
    assert ProposalNumber.zero() == ProposalNumber.zero()
    assert ProposalNumber(1, "a") == ProposalNumber(1, "a")
    assert ProposalNumber(1, "a") > ProposalNumber.zero()
    assert ProposalNumber(1, "b") > ProposalNumber(1, "a")


def test_acceptor_initial_state() -> None:
    a = Acceptor()
    assert a.state.min_proposal == ProposalNumber.zero()
    assert a.state.accepted_proposal == ProposalNumber.zero()
    assert a.state.accepted_value is None
    assert a.state.max_round == 0


def test_acceptor_prepare_new_proposal() -> None:
    a = Acceptor()
    pn = ProposalNumber(1, "p1")
    pr = a.on_prepare(PrepareRequest(pn))
    assert pr.accepted_proposal == ProposalNumber.zero()
    assert pr.accepted_value is None
    assert a.state.min_proposal == pn


def test_acceptor_prepare_old_proposal() -> None:
    a = Acceptor()
    pn2 = ProposalNumber(2, "p2")
    pn1 = ProposalNumber(1, "p1")
    pr = a.on_prepare(PrepareRequest(pn2))
    assert pr.accepted_proposal == ProposalNumber.zero()
    assert pr.accepted_value is None
    assert a.state.min_proposal == pn2
    pr = a.on_prepare(PrepareRequest(pn1))
    assert pr.accepted_proposal == ProposalNumber.zero()
    assert pr.accepted_value is None
    assert a.state.min_proposal == pn2


def test_acceptor_accept_same_propsal_as_prepare() -> None:
    a = Acceptor()
    pn = ProposalNumber(1, "p1")
    pr = a.on_prepare(PrepareRequest(pn))
    assert pr.accepted_proposal == ProposalNumber.zero()
    assert pr.accepted_value is None
    ar = a.on_accept(AcceptRequest(pn, Value("foo")))
    assert ar.min_proposal == pn


def test_acceptor_accept_newer_proposal_than_prepare() -> None:
    a = Acceptor()
    pn1 = ProposalNumber(1, "p1")
    pn2 = ProposalNumber(2, "p2")
    pr = a.on_prepare(PrepareRequest(pn1))
    assert pr.accepted_proposal == ProposalNumber.zero()
    assert pr.accepted_value is None
    ar = a.on_accept(AcceptRequest(pn2, Value("bar")))
    assert ar.min_proposal == pn2


class MockTransport(Transport):
    def __init__(self) -> None:
        self.prepare_requests: dict[AcceptorId, PrepareRequest] = {}
        self.prepare_callbacks: dict[AcceptorId, PrepareCallback] = {}
        self.accept_requests: dict[AcceptorId, AcceptRequest] = {}
        self.accept_callbacks: dict[AcceptorId, AcceptCallback] = {}

    def send_prepare_request(
        self,
        acceptor_id: AcceptorId,
        prepare_request: PrepareRequest,
        callback: PrepareCallback,
    ) -> None:
        self.prepare_requests[acceptor_id] = prepare_request
        self.prepare_callbacks[acceptor_id] = callback

    def send_accept_request(
        self,
        acceptor_id: AcceptorId,
        accept_request: AcceptRequest,
        callback: AcceptCallback,
    ) -> None:
        self.accept_requests[acceptor_id] = accept_request
        self.accept_callbacks[acceptor_id] = callback


def test_proposer() -> None:
    a1, a2, a3 = AcceptorId("a1"), AcceptorId("a2"), AcceptorId("a3")
    acceptor_ids = [a1, a2, a3]
    mock_transport = MockTransport()
    proposer = Proposer(
        server_id="p1",
        transport=mock_transport,
        acceptor_ids=acceptor_ids,
    )
    assert proposer.state.max_round == 0
    value = Value("foo")

    def check(written_value: Value) -> None:
        assert written_value == value

    proposer.write(value=value, callback=check)
    assert proposer.state.max_round == 1
    # Check that we sent a new proposal with incremented round number
    proposal_number = ProposalNumber(1, "p1")
    assert set(acceptor_ids) == mock_transport.prepare_requests.keys()
    assert all(
        pr.proposal_number == proposal_number
        for pr in mock_transport.prepare_requests.values()
    )
    # Now a1 replies to PrepareRequest, and it's the first prepare it sees
    mock_transport.prepare_callbacks[a1](
        a1,
        PrepareResponse(accepted_proposal=ProposalNumber.zero(), accepted_value=None),
    )
    # And the proposer should do nothing since no majority was reached
    assert len(proposer.prepare_responses) == 1
    assert len(mock_transport.accept_requests) == 0
    # Now a2 replies to PrepareRequest, and it's the first prepare it sees
    mock_transport.prepare_callbacks[a2](
        a2,
        PrepareResponse(accepted_proposal=ProposalNumber.zero(), accepted_value=None),
    )
    # TODO: what to do about a3? See TODO in `_on_prepare_response`
    # At this point, the proposer should start sending Accept requests to all
    # acceptors using the initial proposal number, as no acceptor saw a higher one
    assert len(mock_transport.accept_requests) == 3
    assert set(acceptor_ids) == mock_transport.accept_requests.keys()
    assert all(
        ar.proposal_number == proposal_number
        for ar in mock_transport.accept_requests.values()
    )
    # Now a1 replies to AcceptRequest
    mock_transport.accept_callbacks[a1](a1, AcceptResponse(proposal_number))
    # Now a2 replies to AcceptRequest
    mock_transport.accept_callbacks[a2](a2, AcceptResponse(proposal_number))
