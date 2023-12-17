# Elastickv

## Overview
Elastickv is an experimental project undertaking the challenge of creating a distributed key-value store optimized for cloud environments, in a manner similar to DynamoDB. This project is currently in the planning and development phase, with the goal to incorporate advanced features like Raft-based data replication, dynamic node scaling, and automatic hot spot re-allocation. Elastickv aspires to be a next-generation cloud data storage solution, combining efficiency with scalability.

## Planned Features
- **Raft-based Data Replication**: Intends to implement data consistency and fault tolerance using the Raft consensus algorithm for distributed systems.
- **Dynamic Node Scaling**: Aims to dynamically adjust nodes and partition keys in response to variable loads, inspired by DynamoDB's cloud scalability.
- **Automatic Hot Spot Re-allocation**: Plans to incorporate the capability to identify and reallocate hot spots in real-time, to improve system performance and efficiency in cloud settings.

## Development Status
Elastickv is in the experimental and developmental phase, aspiring to bring to life features that resonate with industry standards like DynamoDB, tailored for cloud infrastructures. We welcome contributions, ideas, and feedback as we navigate through the intricacies of developing a scalable and efficient cloud-optimized distributed key-value store.



### Development


### Setup pre-commit hooks
```bash
git config --local core.hooksPath .githooks
```
