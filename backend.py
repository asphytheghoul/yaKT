from flask import Flask, jsonify, request
import threading
from uuid import uuid4
from datetime import datetime

app = Flask(__name__)

# Your in-memory data storage, you may replace it with a database in a real-world scenario
metadata_storage = {
        "RegisterBrokerRecord": [
        {
            "type": "metadata",
            "name": "RegisterBrokerRecord",
            "fields": {
                "internalUUID": "", 
                "brokerId": 0,
                "brokerHost": "", 
                "brokerPort": "", 
                "securityProtocol": "", 
                "brokerStatus": "", 
                "rackId": "", 
                "epoch": 0 
            }
        },
        {
            "type": "metadata",
            "name": "RegisterBrokerRecord",
            "fields": {
                "internalUUID": "", 
                "brokerId": 1,
                "brokerHost": "", 
                "brokerPort": "", 
                "securityProtocol": "", 
                "brokerStatus": "", 
                "rackId": "", 
                "epoch": 0 
            }
        }
    ],
"TopicRecord":{
	"type": "metadata",
	"name": "TopicRecord",
	"fields": {
		"topicUUID": "",
		"name": "",
	},
	"timestamp": "" 
},
"PartitionRecord":{
	"type": "metadata",
	"name": "PartitionRecord",
	"fields": {
		"partitionId": 0,  
		"topicUUID": "",   
        "replicas": [],  
		"ISR": [],   
		"removingReplicas": [],  
		"addingReplicas": [], 
		"leader": "",  
		"partitionEpoch": 0  
	},
	"timestamp": "" 
},
"ProducerIdsRecord":{
	"type": "metadata",
	"name": "ProducerIdsRecord",
	"fields": {
		"brokerId": "", 
		"brokerEpoch": 0,  
		"producerId": 0  
	},
	"timestamp": "" 
},
"BrokerRegistrationChangeBrokerRecord":{
	"type": "metadata",
	"name": "RegistrationChangeBrokerRecord",
	"fields": {
		"brokerId": "", 
		"brokerHost": "", 
		"brokerPort": "", 
		"securityProtocol": "", 
		"brokerStatus": "", 
		"epoch": 0
	},
	"timestamp": "" 
}

}

# Mutex for thread-safety
lock = threading.Lock()

@app.route('/get_metadata', methods=['GET'])
def get_metadata():
    with lock:
        # Retrieve the current state of metadata_storage
        metadata = metadata_storage
        return jsonify({"status": "success", "metadata": metadata}), 200

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
@app.route('/')
def hello_world():
    
    return 'Hello, World!'

@app.route('/create_broker',methods = ['POST'])
def create_broker():
    
        # Get JSON data from the request
        data = request.get_json()

        # Check if "brokerId" is present in the data
        if "brokerId" not in data:
            return jsonify({"status": "error", "message": "BrokerId is required"}), 400
        broker_uuid = str(uuid4()) 
        broker_id = int(data["brokerId"])

       

        # Use a lock to ensure thread safety when updating metadata_storage
        with lock:
            new_broker_id = {
            "type": "metadata",
            "name": "RegisterBrokerRecord",
            "fields": {
                "internalUUID": broker_uuid, 
                "brokerId": broker_id,
                "brokerHost": "", 
                "brokerPort": "", 
                "securityProtocol": "", 
                "brokerStatus": "", 
                "rackId": "", 
                "epoch": 0 
            }
        }
            metadata_storage["RegisterBrokerRecord"].append(new_broker_id)

        return jsonify({"status": "success", "message": "Broker created successfully"}), 201

@app.route('/delete_broker',methods = ['POST'])
def delete_broker():
        
    data = request.get_json()

   
    if "brokerId" not in data:
        return jsonify({"status": "error", "message": "BrokerId is required"}), 400
    broker_id = int(data["brokerId"])

    # Use a lock to ensure thread safety when updating metadata_storage
    with lock:
        # Find the index of the broker with the given ID in metadata_storage
        index_to_remove = None
        for i, broker_record in enumerate(metadata_storage["RegisterBrokerRecord"]):
            if broker_record["fields"]["brokerId"] == broker_id:
                index_to_remove = i
                break

       
        if index_to_remove != None:
            del metadata_storage["RegisterBrokerRecord"][index_to_remove]
            return jsonify({"status": "success", "message": f"Broker with ID {broker_id} deleted successfully"}), 200
        else:
            return jsonify({"status": "error", "message": f"Broker with ID {broker_id} not found"}), 404
        
        
@app.route('/get_all_brokers', methods=['GET'])
def get_all_brokers():
    with lock:
        broker_ids = [record["fields"]["brokerId"] for record in metadata_storage["RegisterBrokerRecord"]]
        return jsonify({"status": "success", "data": broker_ids}), 200

@app.route('/get_broker/<brokerId>', methods=['GET'])
def get_broker_by_id(brokerId):
    with lock:
        for record in metadata_storage["RegisterBrokerRecord"]:
            if record["fields"]["brokerId"] == int(brokerId):
                return jsonify({"status": "success", "data": record}), 200
        return jsonify({"status": "failure", "message": "Broker not found"}), 404

@app.route('/create_topic', methods=['POST'])
def create_topic():
    data = request.get_json()
    if "name" not in data:
        return jsonify({"status": "error", "message": "Topic name is required"}), 400

    topic_uuid = str(uuid4())

    with lock:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_topic = {
            "type": "metadata",
            "name": "TopicRecord",
            "fields": {
                "topicUUID": topic_uuid,
                "name": data["name"],
            },
            "timestamp": timestamp
        }

        metadata_storage["TopicRecord"][topic_uuid] = new_topic

        return jsonify({
            "status": "success",
            "message": "Topic created successfully",
            "topicUUID": topic_uuid
        }), 201

@app.route('/create_partition', methods=['POST'])
def create_partition():
    data = request.get_json()
    if "topicUUID" not in data:
        return jsonify({"status": "error", "message": "Topic UUID is required"}), 400

    partition_uuid = str(uuid4())

    with lock:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_partition = {
            "type": "metadata",
            "name": "PartitionRecord",
            "fields": {
                "partitionId": partition_uuid,
                "topicUUID": data["topicUUID"],
                "replicas": [],  # Initialize replicas as an empty list
                "ISR": [],       # Initialize ISR as an empty list
                "removingReplicas": [],  # Initialize removingReplicas as an empty list
                "addingReplicas": [],    # Initialize addingReplicas as an empty list
                "leader": "",             # Initialize leader information
                "partitionEpoch": 0      # Initialize partitionEpoch as 0
            },
            "timestamp": timestamp
        }

        metadata_storage["PartitionRecord"][partition_uuid] = new_partition

        return jsonify({
            "status": "success",
            "message": "Partition created successfully",
            "partitionUUID": partition_uuid
        }), 201

@app.route('/remove_replica', methods=['POST'])
def remove_replica():
    data = request.get_json()
    if "partitionUUID" not in data or "replicaId" not in data:
        return jsonify({"status": "error", "message": "Both partitionUUID and replicaId are required"}), 400

    partition_uuid = data["partitionUUID"]
    replica_id = data["replicaId"]

    with lock:
        if partition_uuid in metadata_storage["PartitionRecord"]:
            removing_replicas = metadata_storage["PartitionRecord"][partition_uuid]["fields"]["removingReplicas"]
            if replica_id in removing_replicas:
                removing_replicas.remove(replica_id)

                metadata_storage["PartitionRecord"][partition_uuid]["fields"]["partitionEpoch"] += 1

                return jsonify({
                    "status": "success",
                    "message": f"Replica {replica_id} removed from partition {partition_uuid}"
                }), 200

    return jsonify({"status": "error", "message": "Partition not found"}), 404

@app.route('/add_replica', methods=['POST'])
def add_replica():
    data = request.get_json()

    if "partitionUUID" not in data or "replicaId" not in data:
        return jsonify({"status": "error", "message": "Both partitionUUID and replicaId are required"}), 400

    partition_uuid = data["partitionUUID"]
    replica_id = data["replicaId"]

    with lock:
        if partition_uuid in metadata_storage["PartitionRecord"]:
            adding_replicas = metadata_storage["PartitionRecord"][partition_uuid]["fields"]["addingReplicas"]
            if replica_id not in adding_replicas:
                adding_replicas.append(replica_id)

                metadata_storage["PartitionRecord"][partition_uuid]["fields"]["partitionEpoch"] += 1

                return jsonify({
                    "status": "success",
                    "message": f"Replica {replica_id} added to partition {partition_uuid}"
                }), 200

    return jsonify({"status": "error", "message": "Partition not found"}), 404

@app.route('/topic/<topic_uuid>', methods=['GET', 'POST'])
def topic_operations(topic_uuid):
    with lock:
        # Check if the topic UUID exists in metadata_storage
        if topic_uuid not in metadata_storage["TopicRecord"]:
            return jsonify({"status": "error", "message": f"Topic with UUID '{topic_uuid}' not found"}), 404

        # Retrieve the topic data
        topic_data = metadata_storage["TopicRecord"][topic_uuid]["fields"]

        # Client can perform various operations based on the HTTP method
        if request.method == 'GET':
            # Return information about the topic
            return jsonify({"status": "success", "data": topic_data}), 200
        elif request.method == 'POST':
            # Example: Create Partition for the specified topic
            partition_uuid = str(uuid4())
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_partition = {
                "type": "metadata",
                "name": "PartitionRecord",
                "fields": {
                    "partitionId": partition_uuid,
                    "topicUUID": topic_data["topicUUID"],
                    "replicas": [],
                    "ISR": [],
                    "removingReplicas": [],
                    "addingReplicas": [],
                    "leader": "",
                    "partitionEpoch": 0
                },
                "timestamp": timestamp
            }
            metadata_storage["PartitionRecord"][partition_uuid] = new_partition

            return jsonify({"status": "success", "message": f"Partition '{partition_uuid}' created for topic '{topic_data['name']}'"}), 201
        else:
            return jsonify({"status": "error", "message": "Invalid HTTP method"}), 405

## producer
@app.route('/register_producer', methods=['POST'])
def create_producer():
    data = request.get_json()
    if "brokerId" not in data:
        return jsonify({"status": "error", "message": "Broker Id is required"}), 400
    
    producer_uuid = str(uuid4())
    with lock:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_producer = {
            "type": "metadata",
            "name": "ProducerIdsRecord",
            "fields": {
                "brokerId": data["brokerId"],
                "brokerEpoch": 0,
                "producerId": producer_uuid
            },
            "timestamp": timestamp
        }

        metadata_storage["ProducerIdsRecord"][producer_uuid] = new_producer

        return jsonify({
            "status": "success",
            "message": "Producer created successfully",
            "producerUUID": producer_uuid
        }), 201

@app.route('/get_all_producers', methods=['GET'])
def get_all_producers():
    with lock:
        producer_ids = list(metadata_storage["ProducerIdsRecord"])[4:]
        return jsonify({"status": "success", "data": producer_ids}), 200

@app.route('/get_producer/<producerId>', methods=['GET'])
def get_producer_by_id(producerId):
    with lock:
        record = metadata_storage["ProducerIdsRecord"]
        if record["fields"]["producerId"] == int(producerId):
            record.pop("fields",None)
            record.pop("name",None)
            record.pop("timestamp",None)
            record.pop("type",None)
            return jsonify({"status": "success", "data": record}), 200
        return jsonify({"status": "failure", "message": "Producer not found"}), 404
     

if __name__ == '__main__':
    # Use a multi-threaded server for production use
    app.run(threaded=True, port=5000,debug=True)
