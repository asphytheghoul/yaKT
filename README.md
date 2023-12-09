# YAKt-Yet-Another-KRaft

YAKt (Yet Another KRaft) is a Python-based project focusing on creating a KRaft-inspired metadata management system designed to replace Zookeeper in the Kafka ecosystem. This project implements Raft consensus algorithms and event-driven architecture to handle metadata for Kafka.

## Overview
This project implements Raft consensus algorithms and event-driven architecture to handle metadata for Kafka. It offers functionalities for leader election, log replication, fault tolerance, and maintaining event logs.

## Key Features

YeKRAFT offers a range of features and capabilities, including:

- Robust Metadata Management: YeKRAFT ensures the reliable storage and retrieval of metadata related to Kafka topics, partitions, brokers, and producers.

- Event-Driven Architecture: The system is built on an event-driven architecture, allowing for efficient real-time updates and responsiveness.

- High Availability: With Raft consensus, YeKRAFT ensures high availability and fault tolerance, making it suitable for mission-critical applications.

- Simplified API: It provides a straightforward API for creating, reading, updating, and deleting metadata records.

## Tech Stack
- Implemented in Python using Flask
- Leveraged `pyraft` library for Raft consensus

## Specifications
- Raft Node functionalities
- Metadata Storage with specific record types
- HTTP Server with CRD APIs

## Installation and Usage
1. Installation
To get started with YeKRAFT, follow these installation steps:

``` bash
pip install kraft
```
2. Running YeKRAFT with 3 nodes

``` bash
python raft_node.py -i 1 -a 127.0.0.1:5010

python raft_node.py -i 2 -a 127.0.0.1:5020 -e 1/127.0.0.1:5010

python raft_node.py -i 3 -a 127.0.0.1:5030 -e 1/127.0.0.1:5010,2/127.0.0.1:5020
```
3. Using the API

YeKRAFT exposes a simple and intuitive API for managing metadata records. Here are the API endpoints:

- GET /get_metadata: Retrieve metadata information.

- POST /register_broker: Register a Kafka broker.

- GET /get_all_brokers: Retrieve all registered brokers.
 
- GET /get_broker/{broker_id}: Retrieve a specific broker.
 
- PUT /delete_broker/{broker_id}: Delete a specific broker.
 
- POST /create_topic: Create a new Kafka topic.
 
- GET /get_topic/{topic_name}: Retrieve a specific topic.
 
- POST /create_partition: Create a new partition.
 
- GET /get_partitions : Retrieve all partitions.
 
- POST /register_producer/{broker_id}: Register a Kafka producer with a specific broker.
 
- GET /get_producer_ids: Retrieve all registered producer IDs.
 
- PUT /broker_registration_change : Update the registration status of a broker.
 
- GET /broker_mgmt/{timestamp}: Retrieve broker management information.
 
- GET /client_mgmt/{timestamp}: Retrieve client management information.

## References
- [KIP-500 Replace Zookeeper with KRaft](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [Working of Zookeeper](https://zookeeper.apache.org/doc/r3.5.4-beta/zookeeperOver.html)
- [KRaft Metadata Records](https://cwiki.apache.org/confluence/display/KAFKA/KIP-746%3A+Revise+KRaft+Metadata+Records#KIP746:ReviseKRaftMetadataRecords-BrokerRegistrationChangeRecord)
- [Confluent : Intro to KRaft (Suggested Read)](https://www.slideshare.net/HostedbyConfluent/introducing-kraft-kafka-without-zookeeper-with-colin-mccabe-current-2022)
- [Hussein Nasser : How does Kafka work?](https://www.youtube.com/watch?v=LN_HcJVbySw)
- [What is Zookeeper and how is it working with Apache Kafka?](https://www.youtube.com/watch?v=t0FDmj4kaIg)
- [Martin Kleppman : Consensus](https://www.youtube.com/watch?v=rN6ma561tak&pp=ygUNemFiIGNvbnNlbnN1cw%3D%3D)
- [Zookeeper Explained](https://www.youtube.com/watch?v=gZj16chk0Ss)
- [Raft Visualization](https://thesecretlivesofdata.com/raft/)
- [Raft Paper & More](https://raft.github.io/)
- [Core Dump : Understanding Raft](https://www.youtube.com/watch?v=IujMVjKvWP4&pp=ygUNemFiIGNvbnNlbnN1cw%3D%3D)
