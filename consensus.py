import asyncio
import random
from messages import NodeState, Message, MessageType

class ConsensusModule:
    def __init__(self, node_id, peers, send_message_func, logger, storage):
        self.node_id = node_id
        self.peers = peers
        self.send_message = send_message_func
        self.logger = logger
        self.storage = storage
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.current_leader = None
        self.votes_received = 0
        self.election_task = None
        self.heartbeat_task = None
        self.pending_commands = {}

    def step_down(self, term):
        if self.current_term < term or self.state != NodeState.FOLLOWER:
            self.logger.info(f"Stepping down to FOLLOWER for term {term}")
        self.current_term = term
        self.state = NodeState.FOLLOWER
        self.voted_for = None
        self.reset_election_timer()

    def reset_election_timer(self):
        if self.election_task:
            self.election_task.cancel()
        timeout = random.uniform(0.150, 0.300)
        self.election_task = asyncio.create_task(self._election_coro(timeout))

    async def _election_coro(self, timeout):
        try:
            await asyncio.sleep(timeout)
            self.start_election()
        except asyncio.CancelledError:
            pass

    def start_election(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.current_leader = None
        self.votes_received = 1
        self.logger.info(f"Starting election for term {self.current_term}")
        self.reset_election_timer()
        
        for peer in self.peers:
            msg = Message(
                sender_id=self.node_id,
                receiver_id=peer,
                term=self.current_term,
                msg_type=MessageType.REQUEST_VOTE,
                payload={}
            )
            self.send_message(msg)

    def handle_message(self, msg: Message):
        if msg.term > self.current_term and msg.msg_type != MessageType.CLIENT_COMMAND:
            self.step_down(msg.term)
        
        if msg.msg_type == MessageType.REQUEST_VOTE:
            self._handle_request_vote(msg)
        elif msg.msg_type == MessageType.VOTE_RESPONSE:
            self._handle_vote_response(msg)
        elif msg.msg_type == MessageType.APPEND_ENTRIES:
            self._handle_append_entries(msg)
        elif msg.msg_type == MessageType.APPEND_RESPONSE:
            self._handle_append_response(msg)
        elif msg.msg_type == MessageType.CLIENT_COMMAND:
            self._handle_client_command(msg)

    def _handle_request_vote(self, msg: Message):
        grant_vote = False
        if msg.term >= self.current_term:
            if self.voted_for in (None, msg.sender_id):
                grant_vote = True
                self.voted_for = msg.sender_id
                self.reset_election_timer()
                self.logger.info(f"Voted for {msg.sender_id} in term {self.current_term}")
                
        response = Message(
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            term=self.current_term,
            msg_type=MessageType.VOTE_RESPONSE,
            payload={"vote_granted": grant_vote}
        )
        self.send_message(response)

    def _handle_vote_response(self, msg: Message):
        if self.state != NodeState.CANDIDATE:
            return
            
        if msg.payload.get("vote_granted"):
            self.votes_received += 1
            if self.votes_received > (len(self.peers) + 1) // 2:
                self.become_leader()

    def become_leader(self):
        self.state = NodeState.LEADER
        self.current_leader = self.node_id
        self.logger.info(f"Became LEADER for term {self.current_term}")
        self.pending_commands.clear()
        if self.election_task:
            self.election_task.cancel()
        self.send_heartbeats()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        try:
            while self.state == NodeState.LEADER:
                self.send_heartbeats()
                await asyncio.sleep(0.050)
        except asyncio.CancelledError:
            pass

    def send_heartbeats(self, payload=None):
        data = payload or {}
        for peer in self.peers:
            msg = Message(
                sender_id=self.node_id,
                receiver_id=peer,
                term=self.current_term,
                msg_type=MessageType.APPEND_ENTRIES,
                payload=data
            )
            self.send_message(msg)

    def _handle_append_entries(self, msg: Message):
        if msg.term >= self.current_term:
            self.reset_election_timer()
            self.current_leader = msg.sender_id
            if self.state == NodeState.CANDIDATE:
                self.step_down(msg.term)
        
        success = msg.term >= self.current_term
        
        if success and msg.payload.get("action"):
            cmd_id = msg.payload.get("cmd_id")
            action = msg.payload.get("action")
            key = msg.payload.get("key")
            
            if action == "SET":
                self.storage.set_value(key, msg.payload.get("value"))
            elif action == "DELETE":
                self.storage.delete_value(key)
                
            self.logger.info(f"Replicated {action} on key '{key}'")
            
            response_payload = {"success": True, "cmd_id": cmd_id}
        else:
            response_payload = {"success": success}

        response = Message(
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            term=self.current_term,
            msg_type=MessageType.APPEND_RESPONSE,
            payload=response_payload
        )
        self.send_message(response)

    def _handle_client_command(self, msg: Message):
        client_addr = msg.payload.get("__sender_addr")

        if self.state != NodeState.LEADER:
            self.logger.info(f"Redirecting client to leader: {self.current_leader}")
            response = Message(
                sender_id=self.node_id,
                receiver_id="client",
                term=self.current_term,
                msg_type=MessageType.CLIENT_COMMAND,
                payload={
                    "status": "REDIRECT", 
                    "leader_id": self.current_leader, 
                    "__sender_addr": client_addr
                }
            )
            self.send_message(response)
            return
            
        action = msg.payload.get("action")

        if action == "GET":
            key = msg.payload.get("key")
            value = self.storage.get_value(key)
            self.logger.info(f"Served GET for key '{key}'")
            response = Message(
                sender_id=self.node_id,
                receiver_id="client",
                term=self.current_term,
                msg_type=MessageType.CLIENT_COMMAND,
                payload={"status": "OK", "value": value, "__sender_addr": client_addr}
            )
            self.send_message(response)
            return

        cmd_id = f"{msg.term}_{random.randint(1000,9999)}"
        payload = msg.payload.copy()
        payload["cmd_id"] = cmd_id
        
        self.pending_commands[cmd_id] = {
            "payload": payload,
            "acks": 1 
        }
        
        self.logger.info(f"Received {action} for key '{payload['key']}'")
        self.send_heartbeats(payload=payload)

    def _handle_append_response(self, msg: Message):
        if self.state != NodeState.LEADER:
            return
            
        cmd_id = msg.payload.get("cmd_id")
        if msg.payload.get("success") and cmd_id in self.pending_commands:
            self.pending_commands[cmd_id]["acks"] += 1
            
            if self.pending_commands[cmd_id]["acks"] > (len(self.peers) + 1) // 2:
                payload = self.pending_commands[cmd_id]["payload"]
                action = payload["action"]
                key = payload["key"]
                
                if action == "SET":
                    self.storage.set_value(key, payload.get("value"))
                elif action == "DELETE":
                    self.storage.delete_value(key)
                    
                self.logger.info(f"Committed {action} on key '{key}' to local storage")
                
                client_response = Message(
                    sender_id=self.node_id,
                    receiver_id="client",
                    term=self.current_term,
                    msg_type=MessageType.CLIENT_COMMAND,
                    payload={"status": "OK", "cmd_id": cmd_id, "__sender_addr": payload.get("__sender_addr")}
                )
                self.send_message(client_response)
                
                del self.pending_commands[cmd_id]