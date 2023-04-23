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
ISGBN = 3

class Node:
    def __init__(self, my_port, peer_ports_rcv, peer_ports_send, connections, start=False):
        self.ip = '127.0.0.1'
        self.port = my_port
        self.cons = connections
        self.paths = {}
        self.paths_lock = threading.Lock()
        self.update_wait = threading.Event()
        self.peer_ports_rcv = peer_ports_rcv
        self.peer_ports_send = peer_ports_send
        for c in self.cons:
            self.paths[c] = (self.cons[c], self.port, time.time()) #EDITED THIS
        connect_thread = threading.Thread(target=self.udp_receive_loop, args=())
        connect_thread.start()
        self.gbndict = {}
        for p, c in peer_ports_rcv:
            self.gbndict[p] = Client(my_port, p, 5, c)
            self.gbndict[p].start()
        for p in peer_ports_send:
            self.gbndict[p] = Client(my_port, p, 5, 0)
            self.gbndict[p].start()
        self.started=start
        if start:
            self.start()
    def start(self):
        self.paths_lock.acquire()
        self.broadcastTable()
        self.print_current_table()
        self.paths_lock.release()
        send_thread = threading.Thread(target=self.send_updates, args=())
        send_thread.start()
        connect_thread = threading.Thread(target=self.print_status_update, args=())
        connect_thread.start()
    def send_updates(self):
        for p in self.peer_ports_send:
            send_thread = threading.Thread(target=self.gbndict[p].gbn, args=())
            send_thread.start()
        while(True):
            for p in self.peer_ports_send:
                self.change_connection_weight(p, self.gbndict[p].get_drop_rate(), True)
            self.update_wait.wait(TIMEOUT * 10)
            self.update_wait.clear()
    def udp_receive_loop(self):
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # bind the socket to a public host, and a well-known port
        clientsocket.bind(('', self.port))
        while True:
            clientsocket.setblocking(1)
            first_four_bytes = clientsocket.recvfrom(4,socket.MSG_PEEK)[0]
            content_size = int.from_bytes(first_four_bytes, byteorder='big')
            clientsocket.settimeout(0)
            data = None
            try:
                data, addr = clientsocket.recvfrom(content_size)
            except BlockingIOError:
                continue
            if len(data) ==content_size:
                #valid thing received
                is_cons = int.from_bytes(data[4:6], byteorder='big')
                if is_cons == ISGBN:
                    self.gbndict[int.from_bytes(data[13:17], byteorder='big')].incoming_queue.put(data)
                else:
                    tstamp = data[6:27]
                    sender_port = data[27:31]
                    data = data[31:]
                    process_thread = threading.Thread(target=self.process_incoming_udp, args=(data,sender_port, tstamp,is_cons))
                    process_thread.start()

            else:
                print(chat_string + "$ [bad msg received]")
    def process_incoming_udp(self, udp_bits, sender_port, tstamp,is_cons):
        if is_cons == 0:
            udp_str = udp_bits.decode('ascii')
            timestamp = float.fromhex(tstamp.decode('ascii'))
            recv_table = json.loads(udp_str)
            new_table = {}
            for n in recv_table:
                new_table[int(n)] = recv_table[n]
            print(get_time_string() +'Message received from Node ' + str(int.from_bytes(sender_port, 'big')) + ' to Node ' + str(self.port),flush=True)
            self.process_incoming_table(new_table, int.from_bytes(sender_port, 'big'), timestamp)
        elif is_cons == 1:
            timestamp = float.fromhex(tstamp.decode('ascii'))
            new_weight = float.fromhex(udp_bits.decode('ascii'))
            self.change_connection_weight(int.from_bytes(sender_port, 'big'), new_weight, from_outside=False)

    def process_incoming_table(self, new_table,sender_port,timestamp):
        has_changed = False
        self.paths_lock.acquire()
        for endpoint in new_table:
            if endpoint == self.port:
                continue
            if endpoint not in self.paths:
                self.paths[endpoint] = (addCosts(new_table[endpoint][0], self.paths[sender_port][0]), sender_port, timestamp)
                has_changed = True
                #self.gbndict[endpoint] = Client(my_port, endpoint, 5, 1000)
                #self.gbndict[endpoint].start()
            else:
                current_cost = round(self.paths[endpoint][0], 5)
                new_cost = round(addCosts(new_table[endpoint][0], self.paths[sender_port][0]), 5)
                if new_cost < current_cost and self.paths[endpoint][1] != sender_port:
                    has_changed = True
                    self.paths[endpoint] = (new_cost, sender_port,timestamp)
                elif self.paths[endpoint][1] == sender_port and self.paths[endpoint][2] < timestamp:
                    oldtstamp = self.paths[endpoint][2]
                    if endpoint not in self.cons:
                        self.paths[endpoint] = (new_cost, sender_port,timestamp)
                    elif new_cost < self.cons[endpoint]:
                        self.paths[endpoint] = (new_cost, sender_port,timestamp)
                    else:
                        self.paths[endpoint] =(round(self.cons[endpoint], 5), self.port,timestamp)
                    if timestamp > oldtstamp:
                        has_changed = True 
        if has_changed or not self.started:
            self.print_current_table()
            if not self.started:
                send_thread = threading.Thread(target=self.send_updates, args=())
                send_thread.start()
                connect_thread = threading.Thread(target=self.print_status_update, args=())
                connect_thread.start()
                self.broadcastTable(None)
            else:
                self.broadcastTable(sender_port)
            self.started = True
        self.paths_lock.release()
    def broadcastTable(self, donotsend=None):
        #IMPORTANT: Must be called within a paths_lock lock context. Does not lock on its own
        message = (self.port).to_bytes(4, byteorder='big') + bytes((json.dumps(self.paths)), 'ascii')
        for node in self.cons:
            if donotsend and node == donotsend:
                continue
            self.smartsend(message, node)
            print(get_time_string() +'Message sent from Node ' + str(self.port) + ' to Node ' + str(node),flush=True)
    def smartsend(self,mesg,sender_port, is_cons = 0):
        #<size> <is-cons> <timestemp> <port> <message>
        timest = bytes(time.time().hex(), 'ascii')
        msg = (is_cons).to_bytes(2, byteorder='big')+ timest + mesg
        byte_length=len(msg)
        #send out its length first
        message = (byte_length + 4).to_bytes(4, byteorder='big') + msg
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message, (self.ip, sender_port))
    def print_current_table(self):
        #IMPORTANT: Must be called within a paths_lock lock context. Does not lock on its own
        print(get_time_string() + 'Node '+ str(self.port) +' Routing Table',flush=True)
        for entry in self.paths:
            hop_string = ' ; Next hop -> Node ' + str(self.paths[entry][1]) if self.paths[entry][1] in self.cons else ''
            print(' - (' + str(self.paths[entry][0]) + ') -> Node ' + str(entry) + hop_string,flush=True) 
    def print_status_update(self):
        while True:
            time.sleep(1)
            for p in self.peer_ports_send:
                print(get_time_string() +  'Link to ' + str(p) +': ' + str(self.gbndict[p].sent) +'packets sent, ' + str(self.gbndict[p].sent - self.gbndict[p].received) +' packets lost, loss rate ' +str(self.gbndict[p].get_drop_rate()))
            
    def change_connection_weight(self, peer_port, new_weight, from_outside=True):
        self.paths_lock.acquire()
        diff =  new_weight - self.cons[peer_port] 
        if abs(diff) > .005:
            for p in self.paths:
                cost, s_port, tstamp = self.paths[p]
                if s_port == peer_port:
                    self.paths[p] = (cost + diff, s_port, time.time())
                elif p == peer_port and s_port == self.port:
                    self.paths[p] = (cost + diff, s_port, time.time())
            self.cons[peer_port] = new_weight
            if from_outside:
                self.informConnection(peer_port)
            self.print_current_table()
            self.broadcastTable()
        self.paths_lock.release()
    def informConnection(self, peer_port):
         message = (self.port).to_bytes(4, byteorder='big') + bytes(self.cons[peer_port].hex(), 'ascii')
         self.smartsend(message, peer_port, 1)



TIMEOUT= 0.5

class Client:
    def __init__(self, port, peer_port, window_size, drop_rate):
        self.ws = window_size
        self.port = port
        self.peer_port = peer_port
        self.cur_pack = 1
        self.drop_rate = drop_rate
        self.ip = '127.0.0.1'
        self.received = 0
        self.sent = 0
        self.incoming_queue = queue.Queue()
        
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
        connect_thread = threading.Thread(target=self.udp_receive_loop, args=())
        connect_thread.start()
        input_thread = threading.Thread(target=self.buffer_send_loop, args=())
        input_thread.start()
    def get_drop_rate(self):
        return round( 1- self.received / self.sent, 2) if self.sent > 0 else 0
    def gbn(self, message=None,frame_size=1):
        self.window_base = 0 
        self.inMiddleSend = True
        last_sent = 0
        while True:
            payload = 'a'
            is_first = 1 if last_sent == 0 else 0
            self.add_to_buffer(payload,is_first, 0, (last_sent % self.seq_space))
            last_sent += 1
    def buffer_send_loop(self):
        self.window_base = 0
        while(True):
            self.set_up_buffer()
            if self.buf_lock.acquire() and self.buffer[self.window_base % self.seq_space]:
                self.window_base = self.window_base % self.seq_space
                temp_base = self.window_base
                timer_wait = threading.Event()
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
                    self.counter_lock.acquire()
                    self.sent += 1
                    self.counter_lock.release()
                    self.alreadysent.add(slot)
                    #MOOSEprint(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + msg +' sent', flush=True)
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
    def udp_receive_loop(self):
        while True:
            data = self.incoming_queue.get()
            #determine whether to drop incoming packet
            if random.uniform(0.0,1.0) < self.drop_rate:
                self.cur_pack = 0
            else:
                self.cur_pack = 1
            #valid thing received
            is_gbn = int.from_bytes(data[4:6], byteorder='big')
            seq_num = int.from_bytes(data[6:10], byteorder='big')
            is_ack = int.from_bytes(data[10:11], byteorder='big')
            is_first = int.from_bytes(data[11:12], byteorder='big')
            is_last = int.from_bytes(data[12:13], byteorder='big')
            from_port = int.from_bytes(data[13:17], byteorder='big')
            payload = data[17:].decode('ascii') if len(data) > 16 else ""
            if self.cur_pack == 0 and not is_ack:
                if is_ack:
                    #MOOSEprint(self.get_time_string() + 'ACK' + str(seq_num) + ' discarded.',flush=True)
                    pass
                else:
                    #MOOSEprint(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + payload +' discarded.',flush=True)
                    pass
            else:
                #packet_thread = threading.Thread(target=self.process_packet, args=(seq_num,is_ack, is_done, payload))
                #packet_thread.start()
                self.process_packet(seq_num,is_ack, is_first, is_last, payload)
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
            self.counter_lock.acquire()
            self.received += 1
            self.counter_lock.release()
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
            #MOOSEprint(self.get_time_string() + 'ACK' + str(seq_num) + ' received, window moves to ' + str((seq_num + 1) % self.seq_space),flush=True)
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
                #MOOSEprint(self.get_time_string() + 'Transmission finished, ACK' + str(self.base) +  ' sent' + str(self.base), flush=True)
                return
            elif not self.inMiddle and is_first == 0:
                self.base_lock.release()
                #MOOSEprint(self.get_time_string() + 'Packet' + str(seq_num) +  ' received, however transmission is not undergoing',flush=True)
                return
            elif not self.inMiddle and is_first == 1:
                self.hasFinished = False
                self.inMiddle = True
                self.base = seq_num
            #MOOSEprint(self.get_time_string() + 'packet' + str(seq_num) +  ' ' + data +' received',flush=True)
            if self.base == seq_num:
                self.base = (self.base + 1) % self.seq_space
                self.smartsend(seq_num,True,is_first, is_done, " ")
                #MOOSEprint(self.get_time_string() + 'ACK' + str(seq_num) +  ' sent, expecting packet' + str(self.base), flush=True)
                if is_done == 1:
                    self.counter_lock.acquire()
                    self.inMiddle = False
                    self.print_clear_stats()
                    self.counter_lock.release()
                    self.hasFinished = True
            else:
                self.smartsend((self.base -1) % self.seq_space,True,is_first, 0, " ")
                #MOOSEprint(self.get_time_string() + 'ACK' + str((self.base -1 )% self.seq_space) +  ' sent, expecting packet' + str((self.base) % self.seq_space), is_done,flush=True)
                
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
    def set_up_buffer(self, event = None):
        time.sleep(TIMEOUT / 75)
        if event:
            event.set()

    def smartsend(self, seq_num, is_ack, is_first, is_done, msg=" "):
        byte_length=len(msg)
        #send out its length first
        #print(to_port, seq_num, is_ack, is_first, is_done, msg, flush=True)
        message = (byte_length + 16).to_bytes(4, byteorder='big') + ISGBN.to_bytes(2,byteorder='big') + seq_num.to_bytes(4,byteorder='big') + (is_ack).to_bytes(1, byteorder='big')+ (is_first).to_bytes(1, byteorder='big') + (is_done).to_bytes(1, byteorder='big') + (self.port).to_bytes(4, byteorder='big') + bytes(msg, 'ascii')
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message, (self.ip, self.peer_port))

def kill_client():
    os.kill(os.getpid(), signal.SIGTERM)

        

            

def addCosts(cost1,cost2):
    return cost1 + cost2
    #return 1 - (1-cost1) * (1-cost2)
def get_time_string():
    return '[' + str(time.time()) + '] '



if __name__ == "__main__":
    my_port = 0
    receiving_ports = [] #tuples of (port, loss rate)
    sending_ports = []
    setofargs = set(sys.argv)
    last = False
    if len(sys.argv) < 5 or 'receive' not in setofargs or 'send' not in setofargs:
        exit('Not enough arguments. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
    try: 
        my_port = int(sys.argv[1])
    except:
        exit('First argument must be a port number. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
    if sys.argv[2] != 'receive':
        exit('Invalid arguments. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
    else:
        arg_ind = 3
        last_port = 0
        try:
            while(arg_ind < len(sys.argv) and sys.argv[arg_ind] != 'send'):
                if arg_ind % 2 == 1:
                    last_port = int(sys.argv[arg_ind])
                else:
                    receiving_ports.append((last_port, float(sys.argv[arg_ind])))
                arg_ind += 1
        except:
            exit('Invalid receive argument types. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')

        if arg_ind % 2 == 0:
            exit('Every neighbor receiving port needs a loss rate. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
        elif sys.argv[arg_ind] != 'send':
            exit('Receive and/or send string not in proper location. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
        elif arg_ind >= len(sys.argv):
            exit('Invalid arguments. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
        arg_ind += 1 # it is even, though
        try:
            while(arg_ind < len(sys.argv) and sys.argv[arg_ind] != 'last'):
                sending_ports.append(int(sys.argv[arg_ind]))
                arg_ind += 1
        except Exception as e:
            exit('Invalid send argument types. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
        if arg_ind != len(sys.argv) and (arg_ind != len(sys.argv) - 1):
            print(arg_ind, len(sys.argv), sys.argv)
            exit('Invalid/additional arguments. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')
        if arg_ind == len(sys.argv) - 1 and sys.argv[arg_ind] == 'last':
            last = True
        if len(receiving_ports) + len(sending_ports) == 0:
            exit('Node must be connected to at least one other node. Usage: python3 cnnode.py <local-port> receive <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... <neighborM-port> <loss-rate-M> send <neighbor(M+1)-port> <neighbor(M+2)-port> ... <neighborN-port> [last]')

        #self, port, peer_ports_rcv=None, peer_ports_send = None
        cons = {}
        for n, cost in receiving_ports:
            cons[n] = 0
        for n in sending_ports:
            cons[n] = 0
        n = Node(my_port, receiving_ports, sending_ports, cons, last)