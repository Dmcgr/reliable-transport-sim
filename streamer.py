from concurrent.futures import ThreadPoolExecutor
from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct
import time
import hashlib
from threading import Lock, Event, Timer


class Streamer:
    # seq_num = 0
    # ack_num = 0


    def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.closed = False
        self.ack_buffer = {}
        self.send_buffer = {}
        self.recv_buffer = {}
        self.seq_number = 0
        self.ack_number = 0
        self.expected_sequence = 0
        self.ack_event = Event()
        self.lock = Lock()
        self.retransmission_timer = None
        self.fin_received = False
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        # since max size of packet is 1472, the max len will be 1472 - header and hash length
        # header length is 9, hash length is 16, so size of data chunks is 1447
        data_chunks = [data_bytes[i:i + 1447] for i in range(0, len(data_bytes), 1447)]

        for chunk in data_chunks:
            with self.lock:
                while self.seq_number >= self.expected_sequence + 5: # window size of 5
                    self.ack_event.wait(timeout=0.25)
                    self.ack_event.clear()

                packet = struct.pack("!BQ", 0, self.seq_number) + chunk
                final_packet = hashlib.md5(packet).digest() + packet
                self.send_buffer[self.seq_number] = final_packet

            self.socket.sendto(final_packet, (self.dst_ip, self.dst_port))

            with self.lock:
                if self.seq_number == self.expected_sequence:
                    if self.retransmission_timer is not None:
                        self.retransmission_timer.cancel()
                    self.retransmission_timer = Timer(0.25, self.check_retransmission)
                    self.retransmission_timer.start()

                self.seq_number+= 1

        with self.lock:
            while self.expected_sequence < self.seq_number:
                self.ack_event.wait(timeout=0.25)
                self.ack_event.clear()

    def recv(self):
        while True:
            while self.ack_number in self.recv_buffer:
                data = self.recv_buffer.pop(self.ack_number)
                self.ack_number += 1
                return data

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if data:
                    received_checksum = data[:16]
                    received_packet = data[16:]
                    if hashlib.md5(received_packet).digest() == received_checksum:
                        flag, seq_num = struct.unpack("!BQ", received_packet[:9])
                        if flag == 0:
                            chunk = received_packet[9:]
                            self.recv_buffer[seq_num] = chunk
                            self.send_ack(seq_num, addr)
                        elif flag == 1:
                            if seq_num >= self.expected_sequence:
                                self.expected_sequence = seq_num + 1
                                self.ack_event.set()
                                if self.retransmission_timer is not None:
                                    self.retransmission_timer.cancel()
                                    self.retransmission_timer = None
                        elif flag == 2:
                            self.fin_received = True
                            self.send_ack(seq_num, addr)

            except Exception as e:
                print("Listener error:", e)
                self.closed = True

    def send_ack(self, seq_num, addr):
        ack_packet = struct.pack("!BQ", 1, seq_num)
        final_packet = hashlib.md5(ack_packet).digest() + ack_packet
        self.socket.sendto(final_packet, addr)

    def check_retransmission(self):
        if self.expected_sequence < self.seq_number:
            for seq_num in range(self.expected_sequence, self.seq_number):
                packet = self.send_buffer[seq_num]
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            if self.retransmission_timer is not None:
                self.retransmission_timer.cancel()
            self.retransmission_timer = Timer(0.25, self.check_retransmission)
            self.retransmission_timer.start()
        else:
            if self.retransmission_timer is not None:
                self.retransmission_timer.cancel()
                self.retransmission_timer = None

    def close(self) -> None:
        fin_seq_num = self.seq_number
        self.seq_number += 1
        fin_packet = struct.pack("!BQ", 2, fin_seq_num)
        fin_packet_checksum = hashlib.md5(fin_packet).digest() + fin_packet
        self.socket.sendto(fin_packet_checksum, (self.dst_ip, self.dst_port))
        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()
