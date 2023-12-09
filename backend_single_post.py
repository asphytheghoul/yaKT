import requests
import json
import time

# Delay before sending the metadata

registerBroker = {
    "type": "metadata",
    "name": "RegisterBrokerRecord",
    "fields": {
        "internalUUID": "hegde",
        "brokerId": 10,
        "brokerHost": "ashvin",
        "brokerPort": "aakash",
        "securityProtocol": "test4",
        "brokerStatus": "bali",
        "rackId": "chirag",
        "epoch": 10
    }
}

registerBroker = json.dumps(registerBroker)

r = requests.post("http://localhost:5001/request", json={"payload": registerBroker}, headers={'Content-Type': 'application/json'})
print(r.text)
