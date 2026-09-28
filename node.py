import asyncio
import argparse
import logging
from consensus import ConsensusModule
from network import NetworkModule
from storage import StorageModule
from messages import Message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RaftNode:
    def __init__(self, node_id, host, port, peer_directory):
        self.node_id = node_id
        self.logger = logging.getLogger(self.node_id)
        self.peer_directory = peer_directory
        self.peers = list(peer_directory.keys())
        
        self.storage = StorageModule(self.node_id)
        
        self.network = NetworkModule(
            host, 
            port, 
            self.peer_directory, 
            self.handle_message
        )
        
        self.consensus = ConsensusModule(
            self.node_id, 
            self.peers, 
            self.network.send_message,
            self.logger,
            self.storage
        )
        self.logger.info(f"Node initialized on {host}:{port}")

    def handle_message(self, msg: Message):
        self.consensus.handle_message(msg)

    async def run(self):
        await self.network.start()
        self.consensus.reset_election_timer()
        
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    
    args = parser.parse_args()
    
    peers_config = {
        "node_1": ("127.0.0.1", 5001),
        "node_2": ("127.0.0.1", 5002),
        "node_3": ("127.0.0.1", 5003),
    }
    
    if args.id in peers_config:
        del peers_config[args.id]
        
    node = RaftNode(args.id, args.host, args.port, peers_config)
    
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        pass