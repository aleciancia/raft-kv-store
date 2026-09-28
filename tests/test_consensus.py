import unittest
import asyncio
from unittest.mock import MagicMock
from consensus import ConsensusModule
from messages import NodeState, Message, MessageType

class TestConsensusModule(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mock_send = MagicMock()
        self.mock_logger = MagicMock()
        self.mock_storage = MagicMock()
        self.peers = ["node_2", "node_3"]
        self.consensus = ConsensusModule(
            "node_1", 
            self.peers, 
            self.mock_send, 
            self.mock_logger, 
            self.mock_storage
        )

    async def asyncTearDown(self):
        if self.consensus.election_task:
            self.consensus.election_task.cancel()
        if self.consensus.heartbeat_task:
            self.consensus.heartbeat_task.cancel()
        await asyncio.sleep(0)

    async def test_initial_state_is_follower(self):
        self.assertEqual(self.consensus.state, NodeState.FOLLOWER)
        self.assertEqual(self.consensus.current_term, 0)

    async def test_start_election_transitions_to_candidate(self):
        self.consensus.start_election()
        
        self.assertEqual(self.consensus.state, NodeState.CANDIDATE)
        self.assertEqual(self.consensus.current_term, 1)
        self.assertEqual(self.consensus.voted_for, "node_1")
        self.assertEqual(self.consensus.votes_received, 1)
        
        self.assertEqual(self.mock_send.call_count, 2)

    async def test_win_election_with_majority(self):
        self.consensus.start_election()
        
        vote_msg = Message(
            sender_id="node_2",
            receiver_id="node_1",
            term=1,
            msg_type=MessageType.VOTE_RESPONSE,
            payload={"vote_granted": True}
        )
        
        self.consensus.handle_message(vote_msg)
        self.assertEqual(self.consensus.state, NodeState.LEADER)

    async def test_step_down_on_higher_term(self):
        self.consensus.start_election()
        
        high_term_msg = Message(
            sender_id="node_2",
            receiver_id="node_1",
            term=2,
            msg_type=MessageType.APPEND_ENTRIES,
            payload={}
        )
        
        self.consensus.handle_message(high_term_msg)
        self.assertEqual(self.consensus.state, NodeState.FOLLOWER)
        self.assertEqual(self.consensus.current_term, 2)