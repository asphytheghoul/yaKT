from flask import Flask, jsonify, request
from pyraft import raft
import time
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
from telnetlib import Telnet
import json
import base64
#added logging
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

# Added logging to file ----------------------------------------------------------------------------------------------------------------------------------------
file_handler = logging.FileHandler('raft_node2.log')
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
        metadata[key] = json.loads(base64.b64decode(metadata[key]).decode())
    return metadata


# RegisterBrokerRecord  --------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_metadata', methods=['GET'])
def receive_metadata():
    metadata = decoding_metadata(node.data)
    logger.info("Metadata received and stored successfully, metadata: %s", metadata)
    return jsonify({'message': 'Metadata received and stored successfully', 'metadata': metadata}), 200


@app.route('/get_all_brokers', methods=['GET'])
def get_all_brokers():
    metadata = decoding_metadata(node.data)
    if 'RegisterBrokerRecord' in metadata:
        brokers = [record['brokerId'] for record in metadata['RegisterBrokerRecord']]
        logger.info("All brokers received successfully, with information: %s", brokers)
        return jsonify({'metadata': brokers}), 200
    else:
        logger.info("RegisterBrokerRecord not found in node data.")
        return jsonify({'message': 'RegisterBrokerRecord not found in node data.'}), 404

@app.route('/get_broker/<brokerId>', methods=['GET'])
def get_broker(brokerId):
    metadata = decoding_metadata(node.data)
    brokers = [record['brokerId'] for record in metadata['RegisterBrokerRecord']]
    if int(brokerId) in brokers:
        broker = [record for record in metadata['RegisterBrokerRecord'] if record['brokerId'] == int(brokerId)]
        logger.info("Broker received successfully with information: %s", broker)
        return jsonify({'metadata': broker}), 200
    else:
        logger.info("Broker not found.")
        return jsonify({'message': 'Broker not found'}), 404


@app.route('/register_broker', methods=['POST'])
def register_broker():
    global global_metadata
    metadata = request.json  
    metadata["fields"]["internalUUID"] = str(uuid4())


    if node.state == 'l':

        name = metadata["name"]
        if name not in global_metadata:
            global_metadata[name] = []

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        timestamp = timestamp.replace(" ", "_")

        metadata["fields"]["timestamp"] = timestamp
        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])


        logger.info("Broker registered successfully with information: %s", metadata["fields"])

        return jsonify({"message": "Broker registered successfully"}), 200
    else:
        logger.info("Node state is not 'l'. Broker not registered.")
        return jsonify({'message': 'node state is not "l". Broker not registered.'}), 400
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# TopicRecord  ----------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_topic/<TopicName>', methods=['GET'])
def get_topic(TopicName):
    metadata = decoding_metadata(node.data)
    topics = [record['TopicName'] for record in metadata['TopicRecord']]
    if TopicName in topics:
        # print entire record of topic where TopicName matches
        topic = [record for record in metadata['TopicRecord'] if record['TopicName'] == TopicName]
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
        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])

        logger.info("Topic created successfully. topic information: %s", metadata["fields"])
        return jsonify({"message": "Topic created successfully"}), 200
    else:
        logger.info("Node state is not 'l'. Topic not created.")
        return jsonify({'message': 'node state is not "l". Topic not created.'}), 400
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# PartitionRecord  ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_partitions/', methods=['GET'])
def get_partitions():
    metadata = decoding_metadata(node.data)
    if 'PartitionRecord' in metadata:
        partitions = [record['PartitionName'] for record in metadata['PartitionRecord']]
        logger.info("All partitions received successfully. Partitions: %s", partitions)
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

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        timestamp = timestamp.replace(" ", "_")

        metadata["fields"]["timestamp"] = timestamp

        global_metadata[name].append(metadata["fields"])

        send_values_to_node_data(tn, metadata['name'], global_metadata[name])

        logger.info("Partition created successfully. Partition information: %s", metadata["fields"])

        return jsonify({"message": "Partition created successfully"}), 200
    else:
        logger.info("Node state is not 'l'. Partition not created.")
        return jsonify({'message': 'node state is not "l". Partition not created.'}), 400
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# ProducerIdsRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/get_producer_ids', methods=['GET'])
def get_producer_ids():
    metadata = decoding_metadata(node.data)
    if 'ProducerIdsRecord' in metadata:
        producers = [record for record in metadata['ProducerIdsRecord']]
        logger.info("All producers received successfully. Producers: %s", producers)
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

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamp = timestamp.replace(" ", "_")
        metadata["fields"]["timestamp"] = timestamp

        # find brokerId in global_metadata['RegisterBrokerRecord'] and get corresponding epoch
        brokers = [record['brokerId'] for record in global_metadata['RegisterBrokerRecord']]
        if int(brokerId) in brokers:
            broker = [record for record in global_metadata['RegisterBrokerRecord'] if record['brokerId'] == int(brokerId)]
            epoch = broker[0]['epoch']
            logger.info("Producer registered successfully with information: %s", metadata["fields"])
        else:
            logger.info("Broker not found. Producer not registered.")
            return jsonify({'message': 'Broker not found'}), 404
        
        
        # add epoch to metadata
        metadata["fields"]["epoch"] = epoch
        metadata["fields"]["brokerId"] = int(brokerId)
        global_metadata[name].append(metadata["fields"])
        logger.info('Epoch added to metadata. Epoch: %s, Broker ID: %s', epoch,brokerId)
        logger.info("Global Metadata: %s", global_metadata[name])
        send_values_to_node_data(tn, metadata['name'], global_metadata[name])

        return jsonify({"message": "Producer registered successfully"}), 200
    else:
        logger.info("Node state is not 'l'. Producer not registered.")
        return jsonify({'message': 'node state is not "l". Producer not registered.'}), 400
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# BrokerRegistrationChangeBrokerRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/broker_registration_change', methods=['POST'])
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
            global_metadata['RegisterBrokerRecord'][record]['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("Broker registration change received successfully. Broker information: %s", global_metadata['RegisterBrokerRecord'][record])
            send_values_to_node_data(tn, 'RegisterBrokerRecord', global_metadata['RegisterBrokerRecord'])
            
            return jsonify({'metadata': global_metadata['RegisterBrokerRecord'][record]}), 200
        
    return jsonify({'message': 'Broker not found'}), 404
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# BrokerManagementRecord ------------------------------------------------------------------------------------------------------------------------------------------------

@app.route('/broker_mgmt/<timestamp>', methods=['GET'])
def broker_mgmt(timestamp):
    metadata = decoding_metadata(node.data)
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S")
    results = {}

    for metadata_type, records in metadata.items():
        if isinstance(records, list):
            sorted_records = sorted(records, key=lambda record: datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S"))
            most_recent_timestamp = datetime.strptime(sorted_records[-1]['timestamp'], "%Y-%m-%d_%H:%M:%S")

            if most_recent_timestamp - timestamp <= timedelta(minutes=10):
                results[metadata_type] = [record for record in sorted_records if datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S") >= timestamp]
                logger.info("Broker management received successfully within 10 minutes. Broker information: %s", results[metadata_type])
            else:
                logger.info("Broker management received successfully post 10 minutes, receiving entire snapshot. Broker information: %s", records)
                results[metadata_type] = records

    return jsonify(results), 200
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------


# ClientMetadataRecord ------------------------------------------------------------------------------------------------------------------------------------------------
@app.route('/client_mgmt/<timestamp>',methods = ['GET'])
def client_mgmt(timestamp):
    metadata = decoding_metadata(node.data)
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d_%H:%M:%S")
    results = {}

    for metadata_type, records in metadata.items():
        if metadata_type not in ['TopicRecord', 'PartitionRecord', 'RegisterBrokerRecord']:
            continue
        if isinstance(records, list):
            sorted_records = sorted(records, key=lambda record: datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S"))
            most_recent_timestamp = datetime.strptime(sorted_records[-1]['timestamp'], "%Y-%m-%d_%H:%M:%S")

            if most_recent_timestamp - timestamp <= timedelta(minutes=10):
                logger.info("Client management: %s received successfully within 10 minutes. Client information: %s", metadata_type,sorted_records)
                results[metadata_type] = [record for record in sorted_records if datetime.strptime(record['timestamp'], "%Y-%m-%d_%H:%M:%S") >= timestamp]
            else:
                logger.info("Client management: %s received successfully post 10 minutes, receiving entire snapshot. Client information: %s", metadata_type, records)
                results[metadata_type] = records

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
