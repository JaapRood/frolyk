# Kafka Stream Processing library for Node

A Node.js interpretation of the [Kafka Streams Processor API](https://kafka.apache.org/10/documentation/streams/developer-guide/processor-api.html).

Frolyk provides a minimal layer over Kafka, to effectively write, test and run stream processing applications. It follows a `task` based concept, where `sources` (kafka topics) flow through user-defined `processors` to generate results, either back to Kafka or some other store. It aims to enable both **stateless** and **stateful** processing, leveraging Kafka ConsumerGroups to spread these tasks between workers.

```js
import createTask from "../src/task";

const task = createTask();

const locationEvents = task.source("location-events");

task.processor(locationEvents, async (assignment) => {
  // Called when Consumer receives assignment through a rebalance, or manual assignment.__dirname

  // Do any setup work here.

  const countsPerTimeWindow = {}; // connect to Postgres? Fetch a store from somewhere else for local use?

  return async (message, context) => {
    const location = parseLocation(message.value);

    const win = getWindow(location.timestamp);

    const existingCount = countsPerTimeWindow[win] || 0;
    const newCount = existingCount + 1;

    countsPerTimeWindow[win] = newCount;

    // Process a single message
    context.send("location-counts", newCount);
    context.commit();
  };
});

// either start processing by connecting to Kafka
await task.start();

// or inject test messages into the processor to verify your logic
const testInterface = await task.inject([
  { topic: "location-events", partition: 0 },
]);

const testLocation = {
  latitude: 4,
  longitude: 10,
  timestamp: Date.now(),
};
testInterface.inject({
  topic: "location-events",
  partition: 0,
  key: null,
  testLocation,
});

console.log(testInterface.committedMessages); // should contain offset of message
```

## Initial goals

- [ ] Kafka Stream Processor API for Node.js
- [x] `Task` construct to describe processor topologies and processing logic
- [x] Testing of processing logic without requiring a Kafka Cluster
- [x] Propagation of errors
- [ ] Simple logging
- [ ] Very few dependencies: KafkaJS, Long?, Highland
- [x] Idiomatic Node, no straight up copy of Java Processor API.
- [x] 100% Test coverage

## Later goals

- [ ] Simple `Worker` / `App` construct to run multiple tasks in a single process
- [ ] Basic message parsing
- [ ] Support long-running processing jobs (> session timeout through heartbeating / pause & resume)
- [ ] Replace Highland streams with custom Node Streams
- [ ] Very _very_ few dependencies: KafkaJS, Long?
- [ ] Basic store support
- [ ] Basic scheduling / windowing
