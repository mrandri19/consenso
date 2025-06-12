"""Based on:
-   Slides: https://ongardie.net/static/raft/userstudy/paxos.pdf
-   Summary: https://ongardie.net/static/raft/userstudy/paxossummary.pdf
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, NewType, Protocol

# region: proposal number


@dataclass(frozen=True, order=True)
class ProposalNumber:
    round_number: int

    server_id: str
    "Used to resolve ties, can be empty when `round_number` is 0"

    @staticmethod
    def zero() -> ProposalNumber:
        return ProposalNumber(0, "")


# Proposal number zero, eq, ord tests
assert ProposalNumber.zero() == ProposalNumber.zero()
assert ProposalNumber(1, "a") == ProposalNumber(1, "a")
assert ProposalNumber(1, "a") > ProposalNumber.zero()
assert ProposalNumber(1, "b") > ProposalNumber(1, "a")

# endregion

# region server state

Value = NewType("Value", str)


# TODO: should be stored durably to be correct
@dataclass
class State:
    min_proposal: ProposalNumber = ProposalNumber.zero()
    """smallest proposal the server will accept, or 0 if it never received a
    Prepare request."""

    accepted_proposal: ProposalNumber = ProposalNumber.zero()
    "last proposal the server has accepted, or 0 if it never accepted any."

    accepted_value: Value | None = None
    """value from the most recent proposal the server has accepted, or null if
    it never accepted any."""

    max_round: int = 0  # TODO: is it safe to initialize to zero?
    "largest round number the server has seen"


# endregion

# region requests and responses


@dataclass(frozen=True)
class PrepareRequest:
    proposal_number: ProposalNumber
    "a new proposal number"


@dataclass(frozen=True)
class PrepareResponse:
    accepted_proposal: ProposalNumber
    accepted_value: Value | None


@dataclass(frozen=True)
class AcceptRequest:
    proposal_number: ProposalNumber
    "the same proposal number used in Prepare"
    value: Value
    """a value, either the highest numbered one from Prepare responses, or if
    none, then one from a client request"""


@dataclass(frozen=True)
class AcceptResponse:
    min_proposal: ProposalNumber


# endregion

# region acceptor state machine


class Acceptor:
    def __init__(self) -> None:
        self.state = State()

    def on_prepare(self, prepare_request: PrepareRequest) -> PrepareResponse:
        if prepare_request.proposal_number > self.state.min_proposal:
            self.state.min_proposal = prepare_request.proposal_number
        return PrepareResponse(self.state.accepted_proposal, self.state.accepted_value)

    def on_accept(self, accept_request: AcceptRequest) -> AcceptResponse:
        if accept_request.proposal_number >= self.state.min_proposal:
            self.state.accepted_proposal = accept_request.proposal_number
            self.state.accepted_value = accept_request.value
            self.state.min_proposal = accept_request.proposal_number
        return AcceptResponse(self.state.min_proposal)


# Acceptor initial state tests
a = Acceptor()
assert a.state.min_proposal == ProposalNumber.zero()
assert a.state.accepted_proposal == ProposalNumber.zero()
assert a.state.accepted_value is None
assert a.state.max_round == 0

# Acceptor on prepare receives new proposal
a = Acceptor()
pn = ProposalNumber(1, "p1")
pr = a.on_prepare(PrepareRequest(pn))
assert pr.accepted_proposal == ProposalNumber.zero()
assert pr.accepted_value is None
assert a.state.min_proposal == pn

# Acceptor on prepare receives old proposal
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

# Acceptor on accept receives same proposal as prepare
a = Acceptor()
pn = ProposalNumber(1, "p1")
pr = a.on_prepare(PrepareRequest(pn))
ar = a.on_accept(AcceptRequest(pn, Value("foo")))
assert ar.min_proposal == pn

# Acceptor on accept receives newer proposal than prepare
a = Acceptor()
pn1 = ProposalNumber(1, "p1")
pn2 = ProposalNumber(2, "p2")
pr = a.on_prepare(PrepareRequest(pn1))
ar = a.on_accept(AcceptRequest(pn2, Value("bar")))
assert ar.min_proposal == pn2

# endregion

# region proposer state machine

AcceptorId = NewType("AcceptorId", str)

ValueCallback = Callable[[Value], None]
PrepareCallback = Callable[[AcceptorId, PrepareResponse], None]
AcceptCallback = Callable[[AcceptorId, AcceptResponse], None]


class Transport(Protocol):
    def send_prepare_request(
        self,
        acceptor_id: AcceptorId,
        prepare_request: PrepareRequest,
        callback: PrepareCallback,
    ) -> None: ...

    def send_accept_request(
        self,
        acceptor_id: AcceptorId,
        accept_request: AcceptRequest,
        callback: AcceptCallback,
    ) -> None: ...


class Proposer:
    def __init__(
        self, server_id: str, transport: Transport, acceptor_ids: list[AcceptorId]
    ) -> None:
        self.state = State()
        self.server_id = server_id
        self.transport = transport
        self.acceptor_ids = acceptor_ids

        # Late initialized in `write`
        self.callback: ValueCallback
        self.value: Value

        self.prepare_responses: dict[AcceptorId, PrepareResponse] = {}
        self.accept_responses: dict[AcceptorId, AcceptResponse] = {}

    # TODO: unsafe to call write again before callback was called.
    # TODO: handle resetting write state
    def write(self, value: Value, callback: ValueCallback) -> None:
        self.value = value
        self.callback = callback

        # 1. Create a new proposal number
        self.state.max_round = self.state.max_round + 1
        proposal_number = ProposalNumber(self.state.max_round, self.server_id)

        # 2. Broadcast Prepare requests to all acceptor_ids
        prepare_request = PrepareRequest(proposal_number)
        for acceptor_id in self.acceptor_ids:
            self.transport.send_prepare_request(
                acceptor_id, prepare_request, self._on_prepare_response
            )

    def _on_prepare_response(
        self, acceptor_id: AcceptorId, prepare_response: PrepareResponse
    ) -> None:
        self.prepare_responses[acceptor_id] = prepare_response

        majority = len(self.acceptor_ids) // 2 + 1
        if len(self.prepare_responses) < majority:
            return
        # 3. Upon receiving Prepare responses from a majority of acceptors
        # TODO: what happens when we receive the third prepare response?
        # we rebroadcast? Do we just drop it? If so, how to encode that?
        # Setting a flag saying to ignore responses by proposal number?

        max_response = sorted(
            self.prepare_responses.values(),
            key=lambda pr: pr.accepted_proposal,
            reverse=True,
        )[0]
        # If the accepted proposal is not zero, it means that the acceptor has
        # already accepted a value, in which case we should continue with that.
        if max_response.accepted_proposal != ProposalNumber.zero():
            assert max_response.accepted_value is not None
            self.value = max_response.accepted_value
        # otherwise keep self.value unchanged, and try to get that accepted.

        # 4. Broadcast Accept requests to all acceptor_ids
        proposal_number = ProposalNumber(self.state.max_round, self.server_id)
        accept_request = AcceptRequest(proposal_number, self.value)
        for acceptor_id in self.acceptor_ids:
            self.transport.send_accept_request(
                acceptor_id, accept_request, self._on_accept_response
            )

    def _on_accept_response(
        self,
        acceptor_id: AcceptorId,
        accept_response: AcceptResponse,
    ) -> None:
        self.accept_responses[acceptor_id] = accept_response

        # 5. Upon receiving an Accept response with min_proposal
        proposal_number = ProposalNumber(self.state.max_round, self.server_id)
        # If reply.n > n, set maxRound from n, and start over at step 1.
        if accept_response.min_proposal > proposal_number:
            self.state.max_round = accept_response.min_proposal.round_number
            raise NotImplementedError("Start over at step 1")

        majority = len(self.acceptor_ids) // 2 + 1
        if len(self.accept_responses) < majority:
            return
        # 6. Wait until receiving Accept responses for n, from a majority
        # TODO: do I need to check that the value is n?

        # 7. Return v
        self.callback(self.value)


# TODO: I should convert the proposer to a state machine as well


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
    a1, PrepareResponse(accepted_proposal=ProposalNumber.zero(), accepted_value=None)
)
# And the proposer should do nothing since no majority was reached
assert len(proposer.prepare_responses) == 1
assert len(mock_transport.accept_requests) == 0
# Now a2 replies to PrepareRequest, and it's the first prepare it sees
mock_transport.prepare_callbacks[a2](
    a2, PrepareResponse(accepted_proposal=ProposalNumber.zero(), accepted_value=None)
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

# If we didn't crash, it means that the test was succesfull! :)

# TODO: finish

# endregion
