import argparse
import sys
import socket
import queue
import json
import signal
import os
import threading
import random
import time

TIMEOUT= 0.5

class Client:
    def __init__(self, port, peer_port, window_size, is_deterministic, drop_rate):
        self.ws = window_size
        self.port = port
        self.peer_port = peer_port
        self.deterministic = is_deterministic
        self.cur_pack = 1
        self.drop_rate = drop_rate
        self.chat_string ="node> "
        self.ip = '127.0.0.1'
        self.received = 0
        self.dropped = 0
        
        #sender stuff
        self.counter_lock = threading.Lock()
        self.seq_space = (self.ws * 2 + 2)
        
        #sending buffer
        self.buffer =[None] * self.seq_space
        self.window_base = 0
        self.next_buffer = 0
        self.buf_lock = threading.Lock()
        self.buf_wait = threading.Event()
        self.timer_wait = threading.Event()
        self.inMiddleSend = False
        self.alreadysent = set()
        
        #receiver stuff
        self.base = 0
        self.base_lock = threading.Lock()
        self.inMiddle = False
        self.hasFinished=False
        
        
        
    def start(self):
        # create an INET, STREAMing socket
        serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # bind the socket to a public host, and a well-known port
        serversocket.bind(('', self.port))
        connect_thread = threading.Thread(target=self.udp_receive_loop, args=(serversocket,))
        connect_thread.start()
        input_thread = threading.Thread(target=self.input_loop, args=())
        input_thread.start()
        input_thread = threading.Thread(target=self.buffer_send_loop, args=())
        input_thread.start()
    def input_loop(self):
        print(self.chat_string, end='', flush=True)
        inp = input()
        arg_list = inp.split(maxsplit=1)
        if len(arg_list) > 0:
            if arg_list[0] == 'send':
                self.gbn(arg_list[1])
    def gbn(self, message,frame_size=1):
        self.window_base = 0 
        self.inMiddleSend = True
        for last_sent in range(len(message) // frame_size):
            payload = message[(last_sent*frame_size):(last_sent*frame_size + frame_size)]
            is_first = 1 if last_sent == 0 else 0
            is_last = 1 if last_sent == len(message) - 1 else 0
            self.add_to_buffer(payload,is_first, is_last, (last_sent % self.seq_space))
    def buffer_send_loop(self):
        self.window_base = 0
        while(True):
            if self.buf_lock.acquire() and self.buffer[self.window_base % self.seq_space]:
                self.window_base = self.window_base % self.seq_space
                temp_base = self.window_base
                self.timer_wait = threading.Event() #MOOSE
                for i in range(self.ws):
                    slot = (i + self.window_base) % self.seq_space
                    if (self.window_base + self.ws + i) % self.seq_space in self.alreadysent:
                        self.alreadysent.remove((self.window_base + self.ws + i) % self.seq_space)
                    if slot in self.alreadysent:
                        continue
                    if self.buffer[slot] == None:
                        break
                    msg, is_first, is_last, seq_num = self.buffer[slot] #is tuple (msg, is_first, is_last, seq_num)
                    self.smartsend(seq_num, 0, is_first, is_last, msg)
                    self.alreadysent.add(slot)
                    print(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + msg +' sent', flush=True)
                self.buf_lock.release()
                self.buf_wait.set()
                self.timer_wait.clear()
                if not self.timer_wait.wait(TIMEOUT):
                    self.alreadysent = set()
            else:
                self.buf_lock.release()
                self.buf_wait.wait()
    def add_to_buffer(self, msg,is_first, is_last, seq_num):
        while True:
            if self.buf_lock.acquire() and not self.buffer[self.next_buffer]:
                self.buffer[self.next_buffer] = (msg, is_first, is_last, seq_num)
                self.next_buffer = (self.next_buffer + 1) % self.seq_space
                self.buf_lock.release()
                self.buf_wait.set()
                return
            else:
                self.buf_lock.release()
                self.buf_wait.wait(TIMEOUT)
        
        
        
        
        
        
        
        
    def udp_receive_loop(self, clientsocket):
        while True:
            clientsocket.setblocking(1)
            first_four_bytes = clientsocket.recvfrom(4,socket.MSG_PEEK)[0]
            content_size = int.from_bytes(first_four_bytes, byteorder='big')
            clientsocket.settimeout(0)
            data = None
            #determine whether to drop incoming packet
            if self.deterministic:
                self.cur_pack = (self.cur_pack + 1) % self.drop_rate
            elif random.uniform(0.0,1.0) < self.drop_rate:
                self.cur_pack = 0
            else:
                self.cur_pack = 1
            try:
                data, addr = clientsocket.recvfrom(content_size)
            except BlockingIOError:
                continue
            
            if len(data) == content_size and content_size > 9:
                #valid thing received
                seq_num = int.from_bytes(data[4:8], byteorder='big')
                is_ack = int.from_bytes(data[8:9], byteorder='big')
                is_first = int.from_bytes(data[9:10], byteorder='big')
                is_last = int.from_bytes(data[10:11], byteorder='big')
                payload = data[11:].decode('ascii') if len(data) > 11 else ""
                if self.cur_pack == 0 and (self.inMiddle or self.inMiddleSend or is_first == 1):
                    self.counter_lock.acquire()
                    self.dropped += 1
                    self.received += 1
                    self.counter_lock.release()
                    #drop the packet
                    if is_ack:
                        print(self.get_time_string() + 'ACK' + str(seq_num) + ' discarded.',flush=True)
                    else:
                        print(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + payload +' discarded.',flush=True)
                else:
                    #packet_thread = threading.Thread(target=self.process_packet, args=(seq_num,is_ack, is_done, payload))
                    #packet_thread.start()
                    self.process_packet(seq_num,is_ack, is_first, is_last, payload)
                        
            else:
                print(self.get_time_string() + 'Actual bad packet received, could not determine packet contents.',flush=True)
    def process_packet(self,seq_num,is_ack, is_first, is_done, data=None):
        if is_ack:
            if not self.inMiddleSend:
                return
            self.buf_lock.acquire()
            #for i in range(seq_num, self.ws + 1):
            #    if i % self.seq_space in self.alreadysent:
            #        self.alreadysent.remove(i % self.seq_space)
            try:
                if seq_num < self.window_base:
                    for i in range(seq_num, seq_num + self.ws + 1):
                        if i % self.seq_space in self.alreadysent:
                            self.alreadysent.remove(i % self.seq_space)
                else:
                    self.alreadysent.remove(seq_num)
            except:
                pass
            window_end = (self.window_base + self.ws) % self.seq_space
            has_advanced = False
            if window_end > self.window_base and seq_num >= self.window_base and seq_num < window_end:
                has_advanced = True
                for i in range(self.window_base, seq_num + 1):
                    self.buffer[i] = None
            elif window_end < self.window_base and (seq_num >= self.window_base):
                has_advanced = True
                for i in range(self.window_base, seq_num + 1):
                    self.buffer[i] = None
            elif window_end < self.window_base and seq_num < window_end:
                has_advanced = True
                for i in range(0, seq_num + 1):
                    self.buffer[i] = None
            print(self.get_time_string() + 'ACK' + str(seq_num) + ' received, window moves to ' + str((seq_num + 1) % self.seq_space),flush=True)
            self.counter_lock.acquire()
            self.received += 1
            self.counter_lock.release()
            if has_advanced or is_done:
                self.window_base = (seq_num + 1) % self.seq_space
                if is_done == 1:
                    self.print_clear_stats()
                    self.buffer =[None] * self.seq_space
                    self.window_base = 0
                    self.next_buffer = 0
                    self.inMiddleSend = False
                    self.alreadysent = set()
                    kill_client()
                self.buf_lock.release()
            else:
                self.buf_lock.release()
            if not self.timer_wait.is_set():
                self.timer_wait.set()
        else:
            self.base_lock.acquire()
            if self.hasFinished == True:
                self.base_lock.release()
                self.smartsend(self.base,True,is_first, 1, " ")
                print(self.get_time_string() + 'Transmission finished, ACK' + str(self.base) +  ' sent' + str(self.base), flush=True)
                return
            elif not self.inMiddle and is_first == 0:
                self.base_lock.release()
                print(self.get_time_string() + 'Packet' + str(seq_num) +  ' received, however transmission is not undergoing',flush=True)
                return
            elif not self.inMiddle and is_first == 1:
                self.hasFinished = False
                self.inMiddle = True
                self.base = seq_num
            print(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + data +' received',flush=True)
            self.counter_lock.acquire()
            self.received += 1
            self.counter_lock.release()
            if self.base == seq_num:
                self.base = (self.base + 1) % self.seq_space
                self.smartsend(seq_num,True,is_first, is_done, " ")
                print(self.get_time_string() + 'ACK' + str(seq_num) +  ' sent, expecting packet' + str(self.base), flush=True)
                if is_done == 1:
                    self.counter_lock.acquire()
                    self.inMiddle = False
                    self.print_clear_stats()
                    self.counter_lock.release()
                    self.hasFinished = True
            else:
                self.smartsend((self.base -1) % self.seq_space,True,is_first, 0, " ")
                print(self.get_time_string() + 'ACK' + str((self.base -1 )% self.seq_space) +  ' sent, expecting packet' + str((self.base) % self.seq_space), is_done,flush=True)
                
            self.base_lock.release()
            
            
            

    def timer(self, event, timeout=TIMEOUT):
        self.timer_wait.wait(timeout)  
        event.set()       
    def print_clear_stats(self):
        print('[Summary] ' + str(self.dropped) + '/' + str(self.received) + ' packets discarded, loss rate = ' + str(self.dropped/self.received))    
        self.dropped = 0
        self.received = 0  
    def get_time_string(self):
        return '[' + str(time.time()) + '] '
    def smartsend(self, seq_num, is_ack, is_first, is_done, msg=" "):
        byte_length=len(msg)
        #send out its length first
        message = (byte_length + 11).to_bytes(4, byteorder='big') + seq_num.to_bytes(4,byteorder='big') + (is_ack).to_bytes(1, byteorder='big')+ (is_first).to_bytes(1, byteorder='big') + (is_done).to_bytes(1, byteorder='big') + bytes(msg, 'ascii')
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message, (self.ip, self.peer_port))

def kill_client():
    os.kill(os.getpid(), signal.SIGTERM)


#code entrance point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("self_port", help="Port number to listen on", type=int)
    parser.add_argument("peer_port", help="Port number the peer is listening to", type=int)
    parser.add_argument("window_size", help="Window size of GBN protocol", type=int)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d", help="Client Mode",action="store_true")
    group.add_argument("-p", help="Server Mode",action="store_true")
    parser.add_argument("drop_rate", help="Packet drop rate(either probability if -p, or every n packets if -d)",type=float)
    args = parser.parse_args()
    client = None
    if args.d and args.drop_rate > 1:
        client = Client(args.self_port, args.peer_port, args.window_size, True, int(args.drop_rate))
    elif args.p and args.drop_rate < 1 and args.drop_rate >= 0:
        client = Client(args.self_port, args.peer_port, args.window_size, False, args.drop_rate)
    else:
        print("ERROR: If selecting probabilistic mode, please give a decimal between 0.0 and 1.0. If selecting deterministic mode, please give a number greater than 1.")
        exit(0)
    client.start()
        