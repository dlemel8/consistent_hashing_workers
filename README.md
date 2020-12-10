# consistent_hashing_workers
![Test](https://github.com/dlemel8/consistent_hashing_workers/workflows/Verify%20Strategies/badge.svg)

This repo contains 3 services:
* Generator is generating jobs with an id and data
* Workers are processing the jobs and send their results to reporter
* Reporter is aggregating jobs results and then saves a report

In steady state, requirements are:
* Each job id is processed by the same worker
* Workload is evenly distributed among the workers

## Abstract Design
![Abstract](design/abstract.jpg?raw=true "Abstract")

## Implementations
### RabbitMQ (Centralized Message Broker)
![RabbitMQ](design/rabbitmq.jpg?raw=true "RabbitMQ")

### ZeroMQ (Distributed Messaging Sockets)
![ZeroMQ](design/zeromq.jpg?raw=true "ZeroMQ")
