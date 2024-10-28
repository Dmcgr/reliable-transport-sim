# do not import anything else from loss_socket besides LossyUDP
from concurrent.futures.thread import ThreadPoolExecutor

from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.sequence_number = 0 # keep track of the sequence numbers
        self.expected_sequence = 0 # keep track of sequence of received
        self.recv_buffer = {} # buffer for out of order
        self.closed = False # if listener should stop
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def listener(self):
        while not self.closed:
            try:
                received, _ = self.socket.recvfrom()

                # store data in the receive buffer
                seq_num = struct.unpack("Q", received[:8])[0]
                data = received[8:]

                if seq_num >= self.expected_sequence:
                    self.recv_buffer[seq_num] = data

            except Exception as e:
                print("listener died!")
                print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # making the chunks 1024 bytes
        chunk_size = 1024

        for i in range(0, len(data_bytes), chunk_size):
            chunk = data_bytes[i: i + chunk_size]
            header = struct.pack("Q", self.sequence_number) #8B unsigned long long
            chunk = header + chunk

            self.socket.sendto(chunk, (self.dst_ip, self.dst_port))
            self.sequence_number += 1


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!

        while True:
            # the receiving and updating buffer are handled by self.listener
            """
            received, _ = self.socket.recvfrom()

            # get sequence number (header is first 8 bytes)
            seq_num = struct.unpack("Q", received[:8])[0]
            data = received[8:]

            if seq_num == self.expected_sequence:
                self.expected_sequence += 1

                # check if subsequent packets are in buffer
                while self.expected_sequence in self.recv_buffer:
                    data += self.recv_buffer.pop(self.expected_sequence)
                    self.expected_sequence += 1

                return data

            elif seq_num > self.expected_sequence:
                self.recv_buffer[seq_num] = data
            """

            if self.expected_sequence in self.recv_buffer:
                data = self.recv_buffer.pop(self.expected_sequence)
                self.expected_sequence += 1
                return data


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
