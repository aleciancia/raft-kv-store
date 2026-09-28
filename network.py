import asyncio
from messages import Message

class RaftProtocol(asyncio.DatagramProtocol):
    def __init__(self, message_callback):
        self.message_callback = message_callback
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            msg = Message.from_json(data.decode('utf-8'))
            # Catturiamo l'indirizzo del mittente (serve per rispondere al client)
            msg.payload['__sender_addr'] = addr
            self.message_callback(msg)
        except Exception:
            pass

class NetworkModule:
    def __init__(self, host, port, peer_directory, message_callback):
        self.host = host
        self.port = port
        self.peer_directory = peer_directory
        self.message_callback = message_callback
        self.transport = None

    async def start(self):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: RaftProtocol(self.message_callback),
            local_addr=(self.host, self.port)
        )

    def send_message(self, msg: Message):
        if not self.transport:
            return
            
        data = msg.to_json().encode('utf-8')
        
        if msg.receiver_id == "client" and '__sender_addr' in msg.payload:
            # Instradamento speciale di ritorno per il client
            target_addr = tuple(msg.payload['__sender_addr'])
            self.transport.sendto(data, target_addr)
        elif msg.receiver_id in self.peer_directory:
            # Normale traffico tra nodi del cluster
            target_addr = self.peer_directory[msg.receiver_id]
            self.transport.sendto(data, target_addr)