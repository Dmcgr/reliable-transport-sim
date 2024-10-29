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
                received, addr = self.socket.recvfrom()

                # catch really busted packets
                if len(received) < 9:
                    print("Received packet with insufficient length, ignoring.")
                    continue

                # note that first byte is ACK flag
                ack_flag, seq_num = struct.unpack("!BQ", received[:9])
                data = received[9:].strip(b'\x00')


                # if it's an ack, check if it matches with the previous sent seq number
                if ack_flag == 1:
                    print("we got an ack flag")
                    if seq_num == self.sequence_number:
                        self.ack_received = True
                    else:
                        print(f"Received ACK for packet #{seq_num}, but expected #{self.expected_sequence}.")

                # is the FIN packet
                if ack_flag == 2:
                    fin_ack_packet = struct.pack("!BQ", 1, seq_num)
                    self.socket.sendto(fin_ack_packet, addr)
                    self.closed = True
                    print("sent the fin ack")
                    break

                # otherwise, it's data so add to buffer
                else:
                    print("sequence num from packet:", seq_num, "; expected:", self.expected_sequence, "; sequence:", self.sequence_number)
                    if seq_num < self.expected_sequence: # old/duplicate packet, skip
                        print("old/duplicate packet")
                        # ack_packet = struct.pack("!BQ", 1, seq_num)
                        # self.socket.sendto(ack_packet, addr)
                        continue # was previously continue
                    elif seq_num == self.expected_sequence:
                        print(f"Processing packet #{seq_num}.")
                        # self.recv_buffer[seq_num] = data
                        ack_packet = struct.pack("!BQ", 1, seq_num)
                        self.socket.sendto(ack_packet, addr)
                        print("we successfully received, sent ack, and processed the proper packet expected (before inc):",
                              self.expected_sequence)
                        self.expected_sequence += 1

                    else:
                        # Out-of-order packet, add to buffer
                        self.recv_buffer[seq_num] = data
                        print(f"Buffered packet #{seq_num}.")
                        ack_packet = struct.pack("!BQ", 1, seq_num)
                        self.socket.sendto(ack_packet, addr)
                        # self.expected_sequence += 1


            except Exception as e:
                print("Listener died 'cause of this! ", e)


    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!

        # making the chunks 1024 bytes
        chunk_size = 1025

        for i in range(0, len(data_bytes), chunk_size):
            chunk = data_bytes[i: i + chunk_size]
            header = struct.pack("!BQ", 0, self.sequence_number)  # ACK flag + sequence number
            packet = header + chunk
            max_retries = 10
            retry_count = 0
            self.ack_received = False

            while not self.ack_received and retry_count < max_retries:
                print(f"Sending packet #{self.sequence_number}; waiting for ACK...")
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                retry_count += 1
                time.sleep(0.03)

                # Wait for ACK with a timeout for each retry
                start_time = time.time()
                while time.time() - start_time < 0.25:
                    if self.ack_received:  # Exit if ACK confirmed
                        break
                    # else:
                    #     print(f"ACK timeout for packet #{self.sequence_number}. Retrying...")

            # maybe this should be indented
            if self.ack_received:
                print(f"ACK confirmed for packet #{self.sequence_number}")
                self.sequence_number += 1  # Received, go next packet
                # self.expected_sequence += 1
                print("expected:", self.expected_sequence, "; sequence:", self.sequence_number)
                break

            # Max retries, print and go next packet
            if not self.ack_received:
                print(
                    f"Failed to receive ACK for packet #{self.sequence_number} after {max_retries} retries. Skipping packet.")
                self.sequence_number += 1  # Move on even if ACK not received
                # self.expected_sequence += 1
                print("expected:", self.expected_sequence, "; sequence:", self.sequence_number)


    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        data_list = []
        while True:
            # Only process the buffer if it contains the exact expected sequence packet
            if self.expected_sequence in self.recv_buffer:
                # Retrieve and remove the packet from the buffer
                data = self.recv_buffer.pop(self.expected_sequence)
                print(f"Got packet #{self.expected_sequence}.")
                self.expected_sequence += 1  # Increment to the next expected sequence
                return data
            else:
                # Wait if the exact expected sequence packet is not yet available
                time.sleep(0.01)


    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        print("Closing...")
        # Ensure all data is acknowledged
        while not self.ack_received or self.sequence_number > self.expected_sequence:
            time.sleep(0.1)  # Allow time for any final ACKs

        print("Sending FIN packet...")
        fin_packet = struct.pack("!BQ", 2, self.sequence_number)  # ACK flag 2 for FIN
        fin_ack_received = False
        retry_count = 0
        max_fin_retries = 10

        # Send FIN and wait for acknowledgment
        while not fin_ack_received and retry_count < max_fin_retries:
            self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            print("FIN packet sent; awaiting ack...")
            time.sleep(0.1)  # Small delay to wait for possible ACK

            try:
                received, addr = self.socket.recvfrom()
                ack_flag, seq_num = struct.unpack("!BQ", received[:9])

                # Check if it's an ACK for the FIN packet
                if ack_flag == 1 and seq_num == self.sequence_number:
                    fin_ack_received = True
                    print("FIN acknowledgment received.")
            except self.socket.timeout:
                print("Socket timed out waiting for FIN acknowledgment. Retrying...")
            except OSError as e:
                print(f"Socket error occurred: {e}. Retrying...")

            retry_count += 1

        # Final check and cleanup
        if fin_ack_received:
            print("Closing listener after successful FIN acknowledgment...")
        else:
            print("Failed to receive FIN acknowledgment after maximum retries. Proceeding with closure.")

        # Give a grace period before stopping the listener
        time.sleep(1)
        self.socket.stoprecv()