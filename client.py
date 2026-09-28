import socket
import argparse
import json
from messages import Message, MessageType

CLUSTER_CONFIG = {
    "node_1": ("127.0.0.1", 5001),
    "node_2": ("127.0.0.1", 5002),
    "node_3": ("127.0.0.1", 5003),
}

class KVClient:
    def __init__(self, initial_host, initial_port):
        self.current_host = initial_host
        self.current_port = initial_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2.0)

    def send_command(self, action, key, value=None):
        payload = {"action": action, "key": key}
        if value is not None:
            payload["value"] = value
            
        msg = Message(
            sender_id="client",
            receiver_id="any",
            term=0,
            msg_type=MessageType.CLIENT_COMMAND,
            payload=payload
        )
        
        data = msg.to_json().encode('utf-8')
        
        max_redirects = 3
        for _ in range(max_redirects):
            self.sock.sendto(data, (self.current_host, self.current_port))
            
            try:
                response_bytes, _ = self.sock.recvfrom(4096)
                response_data = json.loads(response_bytes.decode('utf-8'))
                
                payload = response_data.get("payload", {})
                
                if payload.get("status") == "REDIRECT":
                    leader_id = payload.get("leader_id")
                    if leader_id and leader_id in CLUSTER_CONFIG:
                        print(f"[!] Redirected to {leader_id}")
                        self.current_host, self.current_port = CLUSTER_CONFIG[leader_id]
                        continue
                    else:
                        print("Error: Leader unknown or unavailable.")
                        return
                        
                print(json.dumps(payload, indent=2))
                return
                
            except socket.timeout:
                print("TIMEOUT")
                return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("action", choices=["SET", "GET", "DELETE"])
    parser.add_argument("key")
    parser.add_argument("value", nargs="?", default=None)
    
    args = parser.parse_args()
    
    client = KVClient(args.host, args.port)
    client.send_command(args.action, args.key, args.value)