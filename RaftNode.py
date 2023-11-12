import random
import time
import threading

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

class RaftNode:
    def __init__(self, nodeID):
        self.nodeID = nodeID
        self.role = FOLLOWER    
        self.logs = []
        self.leaderID = None
        self.currentTerm = 0
        self.commitIndex = 0
        self.voteCount = 0
        self.timeoutThread = None
        self.electionTimeout = self.init_timout()

    def init_timout(self):
        self.reset_election_timeout()

        if self.timeoutThread or self.timeoutThread.is_alive():
            return
        self.timeoutThread = threading.Thread(target=self.election_timeout_check)
        self.timeoutThread.start()
        
    def reset_election_timeout(self):
        return random.randint(150, 300) / 1000 + time.time()
    
    def election_timeout_check(self):
        while self.role == FOLLOWER:
            check = self.electionTimeout - time.time()
            if check < 0:
                self.election_timeout_process()
                break
            time.sleep(check)

    
    def election_timeout_process(self): 
        self.term += 1
        
        # some function to send vote request to other nodes

    
        

