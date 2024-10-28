# do not import anything else from loss_socket besides LossyUDP
from concurrent.futures.thread import ThreadPoolExecutor

from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import time


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
        self.ack_received = False

    def listener(self):
        while not self.closed:
            try:
                received, _ = self.socket.recvfrom()

                # catch really busted packets
                if len(received) < 9:
                    print("Received packet with insufficient length, ignoring.")
                    continue

                # note that first byte is ACK flag
                ack_flag = received[0]
                # print("ACK flag:", ack_flag)
                seq_num = struct.unpack("Q", received[1:9])[0]
                data = received[9:]

                # if it's an ack, check if it matches with the previous sent seq number
                if ack_flag == 1:
                    if seq_num == self.sequence_number - 1:
                        self.ack_received = True

                # otherwise, it's data so add to buffer
                else:
                    if seq_num >= self.expected_sequence:
                        self.recv_buffer[seq_num] = data

            except Exception as e:
                print("Listener died 'cause of this! ", e)


    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # making the chunks 1024 bytes
        chunk_size = 1024

        for i in range(0, len(data_bytes), chunk_size):
            chunk = data_bytes[i: i + chunk_size]

            # add a flag to denote ACK: 1 for yes
            header = struct.pack("BQ", 0, self.sequence_number) #Byte for ACK flag + 8B unsigned long long
            packet = header + chunk
            retry_count = 0
            max_retries = 10
            self.ack_received = False

            while not self.ack_received and retry_count < max_retries:
                # keep trying to send
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                time.sleep(0.01)
                retry_count += 1

            if not self.ack_received:
                print("Failed to receive ACK for packet # ", self.sequence_number)

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

                # keep getting packets that are in order
                while self.expected_sequence in self.recv_buffer:
                    data += self.recv_buffer.pop(self.expected_sequence)
                    self.expected_sequence += 1

                return data


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
