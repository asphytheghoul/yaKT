from flask import Flask, jsonify, request
import threading
from pyraft import raft

node = raft.make_default_node()
node.start()
node.join()

app = Flask(__name__)

# Your in-memory data storage, you may replace it with a database in a real-world scenario
metadata_storage = {
    "RegisterBrokerRecords": {
	"type": "metadata",
	"name": "RegisterBrokerRecord",
	"fields": {
		"internalUUID": "", # type: string
		"brokerId": 0,  # type: int
		"brokerHost": "", # type: string
		"brokerPort": "", # type: string
		"securityProtocol": "", # type: string
		"brokerStatus": "", # type: string
		"rackId": "", # type: string
		"epoch": 0 # type: int; epoch number given by the quorum controller
	}
},
    "TopicRecords": {
	"type": "metadata",
	"name": "TopicRecord",
	"fields": {
		"topicUUID": "",  # type: string
		"name": "" # type: string
	},
	"timestamp": "" # type: timestamp
},
    "PartitionRecords": {
	"type": "metadata",
	"name": "PartitionRecord",
	"fields": {
		"partitionId": 0, # type: int
		"topicUUID": "",  # type: string
		"replicas": [], # type: []int; list of broker IDs with replicas
		"ISR": [], # type: []int; list of insync broker ids 
		"removingReplicas": [], # type: []int; list of replicas in process of removal
		"addingReplicas": [], # type: []int; list of replicas in the process of addition
		"leader": "", # type: string; uuid of broker who is leader for partition
		"partitionEpoch": 0 # type: int; number that incrementatlly changes with changes made to partition
	},
	"timestamp": "" # type: timestamp
},
    "ProducerIdRecords": {
	"type": "metadata",
	"name": "ProducerIdsRecord",
	"fields": {
		"brokerId": "", # type : string; uuid of requesting broker
		"brokerEpoch": 0, # type : int; the epoch at which broker requested
		"producerId": 0 # type : int; producer id requested 
	},
	"timestamp": "" # type: timestamp
},
    "BrokerRegistrationChangeBrokerRecord": {
	"type": "metadata",
	"name": "RegistrationChangeBrokerRecord",
	"fields": {
		"brokerId": "", # type: string
		"brokerHost": "", # type: string
		"brokerPort": "", # type: string
		"securityProtocol": "", # type: string
		"brokerStatus": "", # type: string
		"epoch": 0 # type: int; epoch number given by the quorum controller
	},
	"timestamp": "" # type: timestamp
},
}

# Mutex for thread-safety
lock = threading.Lock()

# Example: RegisterBrokerRecord
@app.route('/register', methods=['POST'])
def register_broker_record():
    data = request.get_json()

    with lock:
        # Your logic to handle the creation of RegisterBrokerRecord
        # Ensure to update the timestamp and return a unique ID
        # ...


    return jsonify({"status": "success", "message": "Broker registered successfully"}), 201

# Example: Get all active brokers
@app.route('/get_all_brokers', methods=['GET'])
def get_all_brokers():
    with lock:
        # Your logic to retrieve all active brokers
        # ...

    return jsonify({"status": "success", "data": brokers}), 200

# Example: Get broker by ID
@app.route('/get_broker/<broker_id>', methods=['GET'])
def get_broker_by_id(broker_id):
    with lock:
        # Your logic to retrieve a broker by ID
        # ...

    return jsonify({"status": "success", "data": broker}), 200

# You should implement similar routes for other record types and actions

if __name__ == '__main__':
    # Use a multi-threaded server for production use
    app.run(threaded=True, port=5000)