from flask import Flask, jsonify, request
from pyraft import raft
import time
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
from telnetlib import Telnet
import json
import base64
import logging

app = Flask(__name__)

# Added logging to file ----------------------------------------------------------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(thread)d] %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# Create the Raft node
node = raft.make_default_node()
global global_metadata
node.start()


global_metadata = {}

# initialize telnet connection
tn = Telnet('localhost', node.port)

#Added logging to file ----------------------------------------------------------------------------------------------------------------------------------------
log_file_path = f'raft_node_{node.port}.log'
file_handler = logging.FileHandler(log_file_path, mode='w') 
formatter = logging.Formatter('%(asctime)s [%(thread)d] %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

def send_values_to_node_data(tn, key, value):
    global global_metadata
    encoded_value = base64.b64encode(json.dumps(value).encode()).decode()

    set_command = f'set {key} {encoded_value}\r\n'
    tn.write(set_command.encode())
    response = tn.read_until(b"\n", timeout = 5).decode().strip()

def decoding_metadata(metadata):
    for key in metadata:
        if key == "ttl":
            continue 
        try:
            metadata[key] = json.loads(base64.b64decode(metadata[key]).decode())
        except:
            pass
    return metadata


# RegisterBrokerRecord  --------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_metadata', methods=['GET'])
def receive_metadata():
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    logger.info("Metadata received and stored successfully, metadata: %s", metadata)
    global_metadata = metadata
    return jsonify({'message': f'Metadata received and stored successfully from node running on port: {node.port}', 'metadata': metadata}), 200


@app.route('/get_all_brokers', methods=['GET'])
def get_all_brokers():
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    if 'RegisterBrokerRecord' in metadata:
        brokers = [record for record in metadata['RegisterBrokerRecord'] if record['brokerStatus'] != "CLOSED"]
        logger.info("All brokers received successfully, with information: %s", brokers)
        return jsonify({'metadata': brokers}), 200
    else:
        logger.info("RegisterBrokerRecord not found in node data.")
        return jsonify({'message': 'RegisterBrokerRecord not found in node data.'}), 404

@app.route('/get_broker/<brokerId>', methods=['GET'])
def get_broker(brokerId):
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    brokers = [record['brokerId'] for record in metadata['RegisterBrokerRecord']]
    if int(brokerId) in brokers:
        broker = [record for record in metadata['RegisterBrokerRecord'] if record['brokerId'] == int(brokerId)]
        logger.info("Broker received successfully, with information: %s", broker)
        return jsonify({'metadata': broker}), 200
    else:
        logger.info("Broker not found.")
        return jsonify({'message': 'Broker not found'}), 404


@app.route('/register_broker', methods=['POST'])
def register_broker():
    global global_metadata
    metadata = request.json  


    if node.state == 'l':
        name = metadata["name"]
        if name not in global_metadata:
            global_metadata[name] = []

        # check if brokerId already exists, if so, dislay error message
        brokers = [record['brokerId'] for record in global_metadata['RegisterBrokerRecord']]
        if int(metadata["fields"]["brokerId"]) in brokers:
            logger.info("Broker %s already registered.",metadata["fields"]["brokerId"])
            return jsonify({"message": f"Broker {metadata['fields']['brokerId']} already registered"}), 200
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S") 
        
        metadata["fields"]["internalUUID"] = str(uuid4())
        metadata["fields"]["timestamp"] = timestamp
        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])
        logger.info("Broker %s registered successfully, with information: %s",metadata["fields"]["internalUUID"], metadata["fields"])

        return jsonify({"message": f"Broker {metadata['fields']['internalUUID']} registered successfully"}), 200
    else:
        logger.info("node state is not 'l'. Broker not registered.")
        return jsonify({'message': 'node state is not "l". Broker not registered.'}), 400
    
@app.route('/delete_broker/<brokerId>',methods = ['PUT'])
def delete_broker(brokerId):
    global global_metadata
    if node.state == 'l':
        for record in range(len(global_metadata['RegisterBrokerRecord'])):
            if int(global_metadata['RegisterBrokerRecord'][record]['brokerId']) == int(brokerId):
                global_metadata['RegisterBrokerRecord'][record]['brokerStatus'] = 'CLOSED'
                global_metadata['RegisterBrokerRecord'][record]['epoch'] += 1
                global_metadata['RegisterBrokerRecord'][record]['timestamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
                send_values_to_node_data(tn, 'RegisterBrokerRecord', global_metadata['RegisterBrokerRecord'])
                logger.info("Broker status changed to CLOSED , with information: %s", global_metadata['RegisterBrokerRecord'][record]['internalUUID'])
                return jsonify({"message": "Broker status changed to CLOSED","brokerUUID":global_metadata['RegisterBrokerRecord'][record]["internalUUID"]}), 200
        logger.info("Broker not found.")
        return jsonify({'message': 'Broker not found'}), 404
    else:
        logger.info("node state is not 'l'. Broker not deleted.")
        return jsonify({'message': 'node state is not "l". Broker not deleted.'}), 400
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# TopicRecord  ----------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_topic/<TopicName>', methods=['GET'])
def get_topic(TopicName):
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    topics = [record['name'] for record in metadata['TopicRecord']]
    if TopicName in topics:
        # print entire record of topic where TopicName matches
        topic = [record for record in metadata['TopicRecord'] if record['name'] == TopicName]
        logger.info("Topic received successfully. topic information: %s", topic)
        return jsonify({'message': 'Topic received successfully', 'metadata': topic}), 200
    else:
        logger.info("Topic not found.")
        return jsonify({'message': 'Topic not found'}), 404
    
    
@app.route('/create_topic', methods=['POST'])
def create_topic():
    global global_metadata
    metadata = request.json
    # print(metadata)
    if node.state == 'l':
        name = metadata["name"]
        if name not in global_metadata:
            global_metadata[name] = []


        # check if topic already exists, if so, display error message
        topics = [record['name'] for record in global_metadata['TopicRecord']]
        if metadata["fields"]["name"] in topics:
            logger.info("Topic %s already exists.",metadata["fields"]["name"])
            return jsonify({"message": f"Topic {metadata['fields']['name']} already exists"}), 200

        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        metadata["fields"]["timestamp"] = timestamp

        # set topicUUID
        metadata["fields"]["topicUUID"] = str(uuid4())

        global_metadata[name].append(metadata["fields"])
        send_values_to_node_data(tn, metadata['name'], global_metadata[name])
        logger.info("Topic created successfully, with information: %s",metadata["fields"])
        return jsonify({"message": f"Topic {metadata['fields']['topicUUID']} created successfully"}), 200
    else:
        logger.info("node state is not 'l'. Topic not created.")
        return jsonify({'message': 'node state is not "l". Topic not created.'}), 400
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# PartitionRecord  ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_partitions', methods=['GET'])
def get_partitions():
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    if 'PartitionRecord' in metadata:
        partitions = [record for record in metadata['PartitionRecord']]
        logger.info("Partitions received successfully, with information: %s", partitions)
        return jsonify({'metadata': partitions}), 200
    else:
        logger.info("PartitionRecord not found in node data.")
        return jsonify({'message': 'PartitionRecord not found in node data.'}), 404


@app.route('/create_partition',methods = ['POST'])
def create_partition():
    global global_metadata
    metadata = request.json
    if node.state == "l":
        name = metadata["name"]
        if name not in global_metadata:
            global_metadata[name] = []

        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S") 

        metadata["fields"]["timestamp"] = timestamp

        # if partitionID already exists, display error message
        partitions = [record['partitionId'] for record in global_metadata['PartitionRecord']]
        if int(metadata["fields"]["partitionId"]) in partitions:
            logger.info("Partition %s already exists.",metadata["fields"]["partitionId"])
            return jsonify({"message": f"Partition {metadata['fields']['partitionId']} already exists"}), 200

        # if topicUUID does not exist, display error message
        topics = [record['topicUUID'] for record in global_metadata['TopicRecord']]
        if metadata["fields"]["topicUUID"] not in topics:
            logger.info("Topic %s does not exist.",metadata["fields"]["topicUUID"])
            return jsonify({"message": f"Topic {metadata['fields']['topicUUID']} does not exist"}), 404

        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])
        logger.info("Partition created successfully, with information: %s", metadata["fields"])

        return jsonify({"message": f"Partition {metadata['fields']['partitionId']} created successfully"}), 200
    else:
        logger.info("node state is not 'l'. Partition not created.")
        return jsonify({'message': 'node state is not "l". Partition not created.'}), 400
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# ProducerIdsRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_producer_ids', methods=['GET'])
def get_producer_ids():
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    if 'ProducerIdsRecord' in metadata:
        producers = [record for record in metadata['ProducerIdsRecord']]
        logger.info("ProducerIdsRecord received successfully, with information: %s", producers)
        return jsonify({'metadata': producers}), 200
    else:
        logger.info("ProducerIdsRecord not found in node data.")
        return jsonify({'message': 'ProducerIdsRecord not found in node data.'}), 404


@app.route('/register_producer/<brokerId>', methods=['POST'])
def register_producer(brokerId):
    global global_metadata
    metadata = request.json
    if node.state == "l":
        name = metadata["name"]
        if name not in global_metadata:
            global_metadata[name] = []
        # check if producerId already exists, if so, display error message
        producers = [record['producerId'] for record in global_metadata['ProducerIdsRecord']]
        if int(metadata["fields"]["producerId"]) in producers:
            logger.info("Producer %s already registered.",metadata["fields"]["producerId"])
            return jsonify({"message": f"Producer {metadata['fields']['producerId']} already registered"}), 200
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        metadata["fields"]["timestamp"] = timestamp

        # find brokerId in global_metadata['RegisterBrokerRecord'] and get corresponding epoch
        brokers = [record['brokerId'] for record in global_metadata['RegisterBrokerRecord']]
        if int(brokerId) in brokers:
            broker = [record for record in global_metadata['RegisterBrokerRecord'] if record['brokerId'] == int(brokerId)]
            if broker[0]['brokerStatus'] == "CLOSED":
                logger.info("Broker %s is closed. Producer not registered.", brokerId)
                return jsonify({'message': f'Broker {brokerId} is closed. Producer not registered.'}), 400
            epoch = broker[0]['epoch']
        else:
            logger.info("Broker %s not found. Producer not registered.", brokerId)
            return jsonify({'message': f'Broker {brokerId} not found'}), 404
        
        # add epoch to metadata
        metadata["fields"]["epoch"] = epoch
        metadata["fields"]["brokerId"] = int(brokerId)
        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])
        logger.info("Producer registered successfully, with information: %s", metadata["fields"])

        return jsonify({"message": f"Producer {metadata['fields']['producerId']} registered successfully"}), 200
    else:
        logger.info("node state is not 'l'. Producer not registered.")
        return jsonify({'message': 'node state is not "l". Producer not registered.'}), 400
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# BrokerRegistrationChangeBrokerRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/broker_registration_change', methods=['PUT'])
def get_broker_registration_change():
    global global_metadata
    metadata = request.json
    for record in range(len(global_metadata['RegisterBrokerRecord'])):
        if int(global_metadata['RegisterBrokerRecord'][record]['brokerId']) == int(metadata['fields']['brokerId']):
            # update brokerhost, brokerport, securityprotocol, brokerstatus
            global_metadata['RegisterBrokerRecord'][record]['brokerHost'] = metadata['fields']['brokerHost']
            global_metadata['RegisterBrokerRecord'][record]['brokerPort'] = metadata['fields']['brokerPort']
            global_metadata['RegisterBrokerRecord'][record]['securityProtocol'] = metadata['fields']['securityProtocol']
            global_metadata['RegisterBrokerRecord'][record]['brokerStatus'] = metadata['fields']['brokerStatus']
            global_metadata['RegisterBrokerRecord'][record]['epoch'] += 1
            global_metadata['RegisterBrokerRecord'][record]['timestamp'] = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

            send_values_to_node_data(tn, 'RegisterBrokerRecord', global_metadata['RegisterBrokerRecord'])
            logger.info("Broker registration changed successfully, with information: %s", global_metadata['RegisterBrokerRecord'][record]['internalUUID'])
            
            return jsonify({'metadata': global_metadata['RegisterBrokerRecord'][record]}), 200
    logger.info("Broker not found.")
    return jsonify({'message': 'Broker not found'}), 404
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# BrokerManagementRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/broker_mgmt/<timestamp>', methods=['GET'])
def broker_mgmt(timestamp):
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S")
    results = {}

    for metadata_type, records in metadata.items():
        if isinstance(records, list):
            sorted_records = sorted(records, key=lambda record: datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S"))
            most_recent_timestamp = datetime.strptime(sorted_records[-1]['timestamp'], "%Y-%m-%d_%H:%M:%S")

            if most_recent_timestamp - timestamp <= timedelta(minutes=10):
                results[metadata_type] = [record for record in sorted_records if datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S") >= timestamp]
            else:
                results[metadata_type] = records
    logger.info("Broker management received successfully, with information: %s", results)
    return jsonify(results), 200
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# ClientMetadataRecord ------------------------------------------------------------------------------------------------------------------------------------------------
@app.route('/client_mgmt/<timestamp>',methods = ['GET'])
def client_mgmt(timestamp):
    global global_metadata
    metadata = decoding_metadata(node.data.copy())
    global_metadata = metadata
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S")
    results = {}

    for metadata_type, records in metadata.items():
        if metadata_type not in ['TopicRecord', 'PartitionRecord', 'RegisterBrokerRecord']:
            continue
        if isinstance(records, list):
            sorted_records = sorted(records, key=lambda record: datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S"))
            most_recent_timestamp = datetime.strptime(sorted_records[-1]['timestamp'], "%Y-%m-%d_%H:%M:%S")

            if most_recent_timestamp - timestamp <= timedelta(minutes=10):
                results[metadata_type] = [record for record in sorted_records if datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S") >= timestamp]
            else:
                results[metadata_type] = records
    logger.info("Client management received successfully, with information: %s", results)
    return jsonify(results), 200

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    time.sleep(1.5)
    if node.state == 'l':
        logger.info("Starting leader node on port %s", node.port)
    else:
        logger.info("Starting follower node on port %s", node.port)
    app.run(debug=False, port=node.port+4)
    node.join()
