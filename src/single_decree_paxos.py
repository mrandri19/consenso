"""Based on:
-   Slides: https://ongardie.net/static/raft/userstudy/paxos.pdf
-   Summary: https://ongardie.net/static/raft/userstudy/paxossummary.pdf
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, NewType, Protocol

# region proposal number and state


@dataclass(frozen=True, order=True)
class ProposalNumber:
    round_number: int

    server_id: str
    "Used to resolve ties, can be empty when `round_number` is 0"

    @staticmethod
    def zero() -> ProposalNumber:
        return ProposalNumber(0, "")


Value = NewType("Value", str)


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

# region acceptor


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

# endregion
