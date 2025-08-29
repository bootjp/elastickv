# Elastickv

## Overview
Elastickv is an experimental project undertaking the challenge of creating a distributed key-value store optimized for cloud environments, in a manner similar to DynamoDB. This project is currently in the planning and development phase, with the goal to incorporate advanced features like Raft-based data replication, dynamic node scaling, and automatic hot spot re-allocation. Elastickv aspires to be a next-generation cloud data storage solution, combining efficiency with scalability.

**THIS PROJECT IS CURRENTLY UNDER DEVELOPMENT AND IS NOT READY FOR PRODUCTION USE.**

## Planned Features
- **Raft-based Data Replication**: Intends to implement data consistency and fault tolerance using the Raft consensus algorithm for distributed systems.
- **Dynamic Node Scaling**: Aims to dynamically adjust nodes and partition keys in response to variable loads, inspired by DynamoDB's cloud scalability.
- **Automatic Hot Spot Re-allocation**: Plans to incorporate the capability to identify and reallocate hot spots in real-time, to improve system performance and efficiency in cloud settings.

## Development Status
Elastickv is in the experimental and developmental phase, aspiring to bring to life features that resonate with industry standards like DynamoDB, tailored for cloud infrastructures. We welcome contributions, ideas, and feedback as we navigate through the intricacies of developing a scalable and efficient cloud-optimized distributed key-value store.


## Example Usage

This section provides sample commands to demonstrate how to use the project. Make sure you have the necessary dependencies installed before running these commands.

### Starting the Server
To start the server, use the following command:
```bash
go run cmd/server/demo.go
```

### Starting the Client

To start the client, use this command:
```bash
go run cmd/client/client.go
```

### Working with Redis
To start the Redis client:
```bash
redis-cli -p 63791
```

#### Setting and Getting Key-Value Pairs
To set a key-value pair and retrieve it:
```bash
set key value
get key
quit
```

### Connecting to a Follower Node
To connect to a follower node:
```bash
redis-cli -p 63792
get key
```

### Redirecting Set Operations to Leader Node
```bash
redis-cli -p 63792
set bbbb 1234
get bbbb
quit

redis-cli -p 63793
get bbbb
quit

redis-cli -p 63791
get bbbb
quit
```


### Development

### Running Jepsen tests

Jepsen tests live in `jepsen/`. Install Leiningen and run tests locally:

```bash
curl -L https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > ~/lein
chmod +x ~/lein
(cd jepsen && ~/lein test)
```

These Jepsen tests execute concurrent read and write operations while a nemesis
injects random network partitions. Jepsen's linearizability checker verifies the
history.



### Setup pre-commit hooks
```bash
git config --local core.hooksPath .githooks
```

