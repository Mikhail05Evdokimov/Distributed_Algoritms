from flask import Flask, request, jsonify
import datetime

PORT = 0
node = None
server = Flask("node")

def init(_PORT, _node):
    global node
    node = _node
    PORT = _PORT
    return server

@server.route('/vote', methods=['GET'])
def vote():
    node.lock.acquire()
    commit = int(request.args.get('commit'))
    term = int(request.args.get('term'))
    leader = int(request.args.get('leader'))
    node.log("Voting with term: " + str(node.term) + " c: " + str(node.commit) + " VS term: " + str(term) + " c: " + str(commit))
    if commit > node.commit:
        node.follow(term, commit, leader)
        node.lock.release()
        return f"1"
    if commit == node.commit and term > node.term:
        node.follow(term, commit, leader)
        node.lock.release()
        return f"1"
    node.lock.release()
    return f"0"

@server.route('/heartbeat', methods=['POST'])
def heartbeat_folower():
    node.last_heartbeat = datetime.datetime.now()
    term = int(request.args.get('term'))
    commit = int(request.args.get('commit'))
    leader = int(request.args.get('leader'))
    if node.leader:
        if commit > node.commit or commit == node.commit and term > node.term:
            node.follow(term, commit, leader)
        else:
            node.log("got heartbeat from enother leader with same term, WTF? " + str(leader))
    else:
        node.follow(term, commit, leader)
        node.log("got heartbeat from " + str(leader))
    
    return f"1"

@server.route('/write', methods=['POST'])
def write_storage():
    key = request.args.get('key')
    value = request.args.get('value')
    if node.leader:
        node.leader_write(key, value)        
    else:
        node.send_to_leader(key, value)
    
    return f"1"

@server.route('/read', methods=['GET'])
def read_storage():
    node.dictionary_lock.acquire()
    response = jsonify(node.dictionary)
    node.dictionary_lock.release()
    return response

@server.route('/append', methods=['POST'])
def append_entries():
    node.last_heartbeat = datetime.datetime.now()
    data = request.form.to_dict(flat=True)
    node.log(str(data))
    if not node.leader:
        node.dictionary_lock.acquire()
        node.dictionary = data.copy()
        node.log("append - " + str(node.dictionary))
        node.dictionary_lock.release()
    return f"1"
        
@server.route('/ask_append', methods=['GET'])
def append_asked():
    n = int(request.args.get('node'))
    if node.leader:
        node.append_one1(n)
    return f"1"
    