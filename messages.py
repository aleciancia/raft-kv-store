import json
from enum import Enum
from dataclasses import dataclass, asdict

class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class MessageType(Enum):
    REQUEST_VOTE = "REQUEST_VOTE"
    VOTE_RESPONSE = "VOTE_RESPONSE"
    APPEND_ENTRIES = "APPEND_ENTRIES"
    APPEND_RESPONSE = "APPEND_RESPONSE"
    CLIENT_COMMAND = "CLIENT_COMMAND"

@dataclass
class Message:
    sender_id: str
    receiver_id: str
    term: int
    msg_type: MessageType
    payload: dict

    def to_json(self) -> str:
        data = asdict(self)
        data['msg_type'] = self.msg_type.value
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        data = json.loads(json_str)
        data['msg_type'] = MessageType(data['msg_type'])
        return cls(**data)
