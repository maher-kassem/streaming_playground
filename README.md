# Streaming playground

A repository to test/play with core streaming functionality, locally. Currently, the streaming stack involves, `python`, `(py)spark`, `Deltalake` and `Kafka`. This repo allows one to test everything streaming such as delta -> delta, kafka -> delta, delta -> kafka and kafka -> kafka.

## Why?
Streaming jobs in a production environment can be hard to fully grasp due to the core functionality being hidden behind abstraction layers and compute_storage happening in e.g. k8s in the cloud. To be able to run the core functionality locally, it allows one to run a streaming job, pause it with e.g, a break point and inspect what is going on on e.g. the storage layer or meta data layer. Questions one could have is e.g., how many parquet files do I expect there to be before and after a `compact` or `optimize` call or what do I expect the delta meta data to look like with "these" settings/options.
## Installation
### Install streaming_playground
To install the `streaming_playground` package, you need `poetry` and run
```
poetry install
```
in the root directory of the repository

### Run kafka locally using docker-compose
There is a docker-compose file in `local_kafka/docker`, that one can simply spin up to host kafka locally

[local_kafka/docker/readme.md](local_kafka/docker/readme.md)


### Run kafka on k8s using rancher desktop
If you want to run kafka without docker and on k8s, follow the following guid;

[local_kafka/rancher/readme.md](local_kafka/rancher/readme.md)

## How to run?
The idea to to write code in [src/streaming_playground/](src/streaming_playground/) that one then runs via pytest. the [tests/conftest.py](tests/conftest.py) file conveniently sets up a spark environment that works with delta and kafka. Using break points, one can inspect the storage layer e.g. before or after a `compact` call or check what happens when changing some options to delta or kafka. Whatever you need to see behind the scenes to better understand what is going on.
