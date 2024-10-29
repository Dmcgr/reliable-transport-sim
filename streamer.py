from concurrent.futures import ThreadPoolExecutor
from lossy_socket import LossyUDP
from socket import INADDR_ANY
import struct
import time
import hashlib
from threading import Lock, Event, Timer


class Streamer:
    seq_num = 0
    ack_num = 0
    header_len = 9
    hash_len = 16
    max_data_len = 1472 - header_len - hash_len
    receive_buffer = {}
    ack_buffer = {}

    def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.closed = False
        self.ack_buffer = {}
        self.send_buffer = {}
        self.receive_buffer = {}
        self.seq_num = 0
        self.ack_num = 0
        self.base = 0
        self.window_size = 5
        self.ack_event = Event()
        self.lock = Lock()
        self.ack_timeout = 0.5
        self.retransmission_timer = None
        self.fin_received = False
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        data_chunks = [data_bytes[i:i + self.max_data_len] for i in range(0, len(data_bytes), self.max_data_len)]

        for chunk in data_chunks:
            with self.lock:
                while self.seq_num >= self.base + self.window_size:
                    self.ack_event.wait(timeout=self.ack_timeout)
                    self.ack_event.clear()

                packet = struct.pack("!BQ", 0, self.seq_num) + chunk
                final_packet = self.pack_checksum(packet)
                self.send_buffer[self.seq_num] = final_packet

            self.socket.sendto(final_packet, (self.dst_ip, self.dst_port))

            with self.lock:
                if self.seq_num == self.base:
                    if self.retransmission_timer is not None:
                        self.retransmission_timer.cancel()
                    self.retransmission_timer = Timer(self.ack_timeout, self.check_retransmission)
                    self.retransmission_timer.start()

                self.seq_num += 1

        with self.lock:
            while self.base < self.seq_num:
                self.ack_event.wait(timeout=self.ack_timeout)
                self.ack_event.clear()

    def recv(self):
        while True:
            while self.ack_num in self.receive_buffer:
                data = self.receive_buffer.pop(self.ack_num)
                self.ack_num += 1
                return data

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if data:
                    received_checksum = data[:self.hash_len]
                    received_packet = data[self.hash_len:]
                    if hashlib.md5(received_packet).digest() == received_checksum:
                        packet_type, seq_num = struct.unpack("!BQ", received_packet[:self.header_len])
                        if packet_type == 0:
                            chunk = received_packet[self.header_len:]
                            self.receive_buffer[seq_num] = chunk
                            self.send_ack(seq_num, addr)
                        elif packet_type == 1:
                            if seq_num >= self.base:
                                self.base = seq_num + 1
                                self.ack_event.set()
                                if self.retransmission_timer is not None:
                                    self.retransmission_timer.cancel()
                                    self.retransmission_timer = None
                        elif packet_type == 2:
                            self.fin_received = True
                            self.send_ack(seq_num, addr)

            except Exception as e:
                print("Listener encountered an error:", e)
                self.closed = True

    def send_ack(self, seq_num, addr):
        ack_packet = struct.pack("!BQ", 1, seq_num)
        final_packet = self.pack_checksum(ack_packet)
        self.socket.sendto(final_packet, addr)

    def pack_checksum(self, packet):
        return hashlib.md5(packet).digest() + packet

    def check_retransmission(self):
        if self.base < self.seq_num:
            for seq_num in range(self.base, self.seq_num):
                packet = self.send_buffer[seq_num]
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            if self.retransmission_timer is not None:
                self.retransmission_timer.cancel()
            self.retransmission_timer = Timer(self.ack_timeout, self.check_retransmission)
            self.retransmission_timer.start()
        else:
            if self.retransmission_timer is not None:
                self.retransmission_timer.cancel()
                self.retransmission_timer = None

    def close(self) -> None:
        fin_seq_num = self.seq_num
        self.seq_num += 1
        fin_packet = struct.pack("!BQ", 2, fin_seq_num)
        fin_packet_checksum = self.pack_checksum(fin_packet)
        self.socket.sendto(fin_packet_checksum, (self.dst_ip, self.dst_port))
        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()
