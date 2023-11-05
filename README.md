<p align="center">
<img src="assets/raft.jpg" width="400" height="300" alt="RAFT"/>
<img src="assets/yacht.jpg" width="400" height="300" alt="YACHT"/>
  <h1>Yet Another Kraft - YeKRAFT (YaKT)</h1>
</p>

## Overview

Yet Another Kraft, or YeKRAFT (YaKT), is a distributed metadata management system developed as part of the Big Data Course at PES University. It is designed to address the need for a modern, efficient, and highly scalable metadata management solution for the Kafka ecosystem.

YeKRAFT leverages the Raft consensus algorithm, a well-established approach for distributed systems, to replace Zookeeper and enhance the reliability and scalability of Kafka's metadata management. This project provides an open-source alternative that can benefit various industries and organizations dealing with large-scale data streaming and event processing.

## Key Features

YeKRAFT offers a range of features and capabilities, including:

- **Robust Metadata Management**: YeKRAFT ensures the reliable storage and retrieval of metadata related to Kafka topics, partitions, brokers, and producers.

- **Event-Driven Architecture**: The system is built on an event-driven architecture, allowing for efficient real-time updates and responsiveness.

- **High Availability**: With Raft consensus, YeKRAFT ensures high availability and fault tolerance, making it suitable for mission-critical applications.

- **Simplified API**: It provides a straightforward API for creating, reading, updating, and deleting metadata records.

## How to Use YeKRAFT

### 1. Installation

To get started with YeKRAFT, follow these installation steps:

```
pip install yekraft
```

### 2. Running YeKRAFT
```
from yekraft import YeKRAFTServer
```
# Create and configure the server
```
server = YeKRAFTServer()
server.configure({
    # Configuration options here
})

# Start the server
server.run()
```

### 3. Using the API
YeKRAFT exposes a simple and intuitive API for managing metadata records. Here are some common API endpoints:

- POST /create_topic: Create a new Kafka topic.
- POST /register_broker: Register a Kafka broker.
- GET /get_metadata: Retrieve metadata information.

## Conclusion
Yet Another Kraft, or YeKRAFT (YaKT), is a promising solution for enhancing the metadata management of Kafka with a robust, highly available, and open-source system. It simplifies the administration of Kafka's metadata and is suitable for a wide range of applications, from data streaming to event processing.

We invite you to explore and use YeKRAFT in your projects and look forward to your contributions and feedback to help make it even better.

Happy data streaming with YeKRAFT!