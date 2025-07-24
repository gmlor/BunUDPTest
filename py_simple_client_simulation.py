import socket
import threading
import time
import random
import struct

RELAY_HOST = '127.0.0.1'
RELAY_PORT = 41234
NUM_CLIENTS = 5
MESSAGE_INTERVAL = 2  # seconds
SIM_DURATION = 30     # seconds total simulation time
MAX_PACKET_SIZE = 1024


class SimulatedClient:
    def __init__(self, id, host, port):
        self.id = id
        self.addr = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', 0))  # OS chooses port
        self.running = True
        self.uuid = 0

    def start(self):
        threading.Thread(target=self.receive_loop, daemon=True).start()

    def send_message(self):
        # Simple format: [type (1 byte)] + [uuid (2 bytes)] + message
        self.uuid = self.uuid or (100 + self.id)
        msg_type = 0x01
        message = f"Hello from client {self.id}".encode()
        packet = struct.pack(">Bh", msg_type, self.uuid) + message
        self.sock.sendto(packet, self.addr)
        print(f"[CLIENT {self.id}] Sent {len(packet)} bytes")

    def receive_loop(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(MAX_PACKET_SIZE)
                print(f"[CLIENT {self.id}] Received: {data}")
            except Exception as e:
                print(f"[CLIENT {self.id}] Error: {e}")
                break

    def stop(self):
        self.running = False
        self.sock.close()


def main():
    clients = [SimulatedClient(i, RELAY_HOST, RELAY_PORT) for i in range(NUM_CLIENTS)]

    for client in clients:
        client.start()

    start_time = time.time()

    while time.time() - start_time < SIM_DURATION:
        for client in clients:
            if random.random() < 0.8:
                client.send_message()
        time.sleep(MESSAGE_INTERVAL)

        # Randomly kill and respawn clients
        if random.random() < 0.3:
            idx = random.randint(0, NUM_CLIENTS - 1)
            clients[idx].stop()
            print(f"[CLIENT {idx}] Simulated disconnect")
            time.sleep(0.5)
            clients[idx] = SimulatedClient(idx, RELAY_HOST, RELAY_PORT)
            clients[idx].start()
            print(f"[CLIENT {idx}] Simulated reconnect")

    print("Stopping all clients...")
    for client in clients:
        client.stop()


if __name__ == '__main__':
    main()
