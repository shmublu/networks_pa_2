import sys
import socket
import queue
import json
import signal
import os
import threading
import random
import time


#code entrance point


class Node:
    def __init__(self, my_port, connections, start=False):
        self.ip = '127.0.0.1'
        self.port = my_port
        self.cons = connections
        self.paths = {}
        self.paths_lock = threading.Lock()
        for c in self.cons:
            self.paths[c] = (self.cons[c], self.port, time.time()) #EDITED THIS
        connect_thread = threading.Thread(target=self.udp_receive_loop, args=())
        connect_thread.start()
        self.started=False
        if start:
            self.start()
    def start(self):
        self.paths_lock.acquire()
        self.broadcastTable()
        self.paths_lock.release()
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



        

            

def addCosts(cost1,cost2):
    return cost1 + cost2
    #return 1 - (1-cost1) * (1-cost2)
def get_time_string():
    return '[' + str(time.time()) + '] '



if __name__ == "__main__":
    if len(sys.argv) < 4:
        exit('Not enough arguments. Usage: python3 dvnode.py <local-port> <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... [last]')
    elif len(sys.argv) % 2 == 0 and sys.argv[-1] == 'last':
        exit('Invalid arguments. Usage: python3 dvnode.py <local-port> <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... [last]')
    else:
        try:
            local_port = int(sys.argv[1])
            connections = {}
            start = False
            for i in range(2, len(sys.argv) if len(sys.argv) % 2 == 0 else len(sys.argv) -1 , 2):
                n_port = int(sys.argv[i])
                n_loss = float(sys.argv[i+1])
                if n_loss >= 1.0 or n_loss < 0:
                    raise Exception('Loss rate must be a between 0.0 and 1.0.')
                connections[n_port] = n_loss
            if len(sys.argv) % 2 == 1 and sys.argv[-1] != 'last':
                 raise Exception('Marker argument must be last.')
            elif len(sys.argv) % 2 == 1:
                start=True

        except IndexError as e:
            exit('Each neighbor port must have a corresponding loss rate. Usage: python3 dvnode.py <local-port> <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... [last]')
        except ValueError as e:
            print(e)
            exit('Invalid argument types. Ports must be integers, and loss rates must be floats. Usage: python3 dvnode.py <local-port> <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... [last]')
        except Exception as e:
            print(e)
            exit('Usage: python3 dvnode.py <local-port> <neighbor1-port> <loss-rate-1> <neighbor2-port> <loss-rate-2> ... [last]')
        n = Node(local_port, connections, start)