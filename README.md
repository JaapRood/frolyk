# Kafka Stream Processing library for Node

Total work in progress, nothing functional yet, but eventually like [Kafka Streams Processor API](https://kafka.apache.org/10/documentation/streams/developer-guide/processor-api.html).


## Initial goals

- [ ] Kafka Stream Processor API for Node.js
- [ ] `Task` construct to describe processor topologies and processing logic
- [ ] Testing of processing logic without requiring a Kafka Cluster
- [ ] Propagation of errors
- [ ] Simple logging
- [ ] Very few dependencies: KafkaJS, Long?, Highland
- [ ] Idiomatic Node, no straight up copy of Java Processor API.
- [x] 100% Test coverage

## Later goals
- [ ] Simple `Worker` / `App` construct to run multiple tasks in a single process
- [ ] Basic message parsing
- [ ] Replace Highland streams with custom Node Streams
- [ ] Very *very* few dependencies: KafkaJS, Long?
- [ ] Basic store support
- [ ] Basic scheduling / windowing