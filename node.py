import requests
import threading
import datetime
import time
import random
import sys
import atomics
import json
import server as serv
from multiprocessing.pool import ThreadPool
nodes = json.loads(sys.argv[2])
PORT = int(sys.argv[1])
RTO = 0.01

class Node():
    
    leader = False
    leader_node = 0
    port = 0
    dictionary = {}
    entries = []
    dictionary_lock = threading.Lock()
    commit = 0
    term = 0
    nodes = []
    timeout = 0.5
    lock = threading.Lock()
    last_heartbeat = 0
    sleep_time = 0.1
    thread_pool = None
    
    def __init__(self, nodes, port):
        self.port = port
        self.thread_pool = ThreadPool(processes=len(nodes)*2+1)
        for n in nodes:
            if n != port:
                self.nodes.append(n)
    
    def start(self):
        self.last_heartbeat = datetime.datetime.now()
        th = threading.Thread(target=server.run, 
                     kwargs={"port":self.port})
        th.start()
        while True:
            if self.leader:
                self.thread_pool.map(self.heartbeat_leader, self.nodes)
                time.sleep(self.sleep_time)
                
            else:
                dt = datetime.datetime.now() - self.last_heartbeat
                #self.log("DT: " + str(dt.total_seconds()))
                if dt.total_seconds() > self.timeout + random.randint(1, 300)/1000:
                    self.leader_election()
                else:
                    time.sleep(self.sleep_time)
            
    def leader_election(self):
        cnt = atomics.atomic(width=4, atype=atomics.INT)
        barrier = threading.Barrier(len(nodes), action=None, timeout=None)
        self.inc_term()
        self.log("START voting with term: " + str(self.term) + " c: " + str(self.commit))
        for n in self.nodes:
            th = threading.Thread(target=self.get_voice, 
            kwargs={"cnt":cnt, "node":n, "barrier":barrier})
            th.start()
        barrier.wait()
        
        if cnt.load() >= len(nodes)//2:
            self.leader = True
            self.append_all()
            #self.log("I'am leader now!")
        self.last_heartbeat = datetime.datetime.now()
            
    def get_voice(self, cnt: atomics.INTEGRAL, node, barrier):
        if cnt.load() >= len(nodes)//2:
            barrier.wait()
            return
        try:
            if int(requests.get('http://localhost:' + str(node) + '/vote?commit=' + str(self.commit) + '&term=' + str(self.term) + '&leader=' + str(self.port), timeout=RTO).text ) == 1: 
                cnt.inc()
        except Exception:
            pass
        barrier.wait()
        
    def log(self, msg):
        print("NODE #" + str(self.port) + " " + msg + " " + "in term - " + str(self.term))
        
    def inc_term(self):
        self.lock.acquire()
        self.term = self.term + 1
        self.lock.release()
        
    def follow(self, term, commit, leader):
        self.term = term
        self.last_heartbeat = datetime.datetime.now()
        self.leader = False
        self.leader_node = leader
        if self.commit < commit:
            try:
                requests.get('http://localhost:' + str(self.leader_node) + '/ask_append?node=' + str(self.port), timeout=RTO)
            except Exception as e:
                print(e)
        #self.commit = commit
        
        
    def heartbeat_leader(self, node):
        try:
            self.log("Send heartbeat to " + str(node) + " " + str(self.commit))
            requests.post('http://localhost:' + str(node) + '/heartbeat?leader=' + str(self.port) + '&commit=' + str(self.commit) + '&term=' + str(self.term), timeout=RTO)
        except Exception:
            pass
    
    def send_to_leader(self, key, value):
        try:
            self.log("Send new value to leader " + str(self.leader_node))
            requests.post('http://localhost:' + str(self.leader_node) + '/write?key=' + key + '&value=' + value, timeout=RTO)
        except Exception:
            pass
                
    def leader_write(self, key, value):
        self.dictionary_lock.acquire()
        self.commit = self.commit + 1
        self.dictionary[key] = value
        self.entries.append(json.loads(Entrie(key, value, self.term).toJson()))
        
        for n in self.nodes:
            try:
                params = "?term=" + str(self.term) + "&commit=" + str(self.commit) + "&leader=" + str(self.port)
                requests.post('http://localhost:' + str(n) + '/append' + params, timeout=RTO, json = self.entries)
            except Exception as e:
                print(e)
        self.dictionary_lock.release()
        self.log("New value in storage")
        
    def append_all(self):
        self.dictionary_lock.acquire()
        self.thread_pool.map_async(self.append_one, self.nodes).wait()
        self.dictionary_lock.release()
        
    def append_one(self, n):
        try:
            params = "?term=" + str(self.term) + "&commit=" + str(self.commit) + "&leader=" + str(self.port)
            requests.post('http://localhost:' + str(n) + '/append' + params, timeout=RTO, json = self.entries)
        except Exception as e:
            pass
        
    def append_one1(self, n):
        self.dictionary_lock.acquire()
        try:
            params = "?term=" + str(self.term) + "&commit=" + str(self.commit) + "&leader=" + str(self.port)
            requests.post('http://localhost:' + str(n) + '/append' + params, timeout=RTO, json = self.entries)
        except Exception as e:
            pass
        self.dictionary_lock.release()
        
class Entrie(json.JSONEncoder):
    
    key = None
    value = None
    term = None
    
    def __init__(self, key, value, term):
        self.key = key
        self.value = value
        self.term = term
        
    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__)
        
node = Node(nodes, PORT)
server = serv.init(PORT, node)
node.start()