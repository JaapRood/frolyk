import Tap from 'tap'
import { Kafka, logLevel } from 'kafkajs'
import H from 'highland'
import Config from './config'
import createStreams, { Message } from '../src/streams'
import Uuid from 'uuid/v4'
import Crypto from 'crypto'
import { spy } from 'sinon'

const secureRandom = (length = 10) =>
    `${Crypto.randomBytes(length).toString('hex')}-${process.pid}-${Uuid()}`

const createConsumer = (options : { logLevel?: logLevel } = {
    logLevel: logLevel.NOTHING
}) => {
    const { logLevel, ...consumerOptions } = options
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers, logLevel })
    

    return kafka.consumer({
        groupId: `group-${secureRandom()}`,
        ...consumerOptions,
        maxWaitTimeInMs: 100
    })
}

const createTopic = async ({ topic, partitions = 1, config = [] }) => {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const admin = kafka.admin()

    try {
        await admin.connect()
        await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic, numPartitions: partitions, configEntries: config }],
        })
    } finally {
        admin && (await admin.disconnect())
    }
}

const deleteTopic = async (topic) => {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const admin = kafka.admin()

    try {
        await admin.connect()
        await admin.deleteTopics({
            topics: [topic]
        })
    } finally {
        admin && (await admin.disconnect())
    }
}

const produceMessages = async (topic, messages) => {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const producer = kafka.producer()

    try {
        await producer.connect()
        await producer.send({ acks: 1, topic, messages })
    } finally {
        producer && (await producer.disconnect())
    }
}

Tap.test('TaskStreams', async (t) => {
    t.test('can construct streams with a KafkaJS Consumer', async (t) => {
        const consumer = createConsumer()
        createStreams(consumer)
    })

    await t.test('TaskStreams.stream', async (t) => {
        let consumer, streams
        t.beforeEach(async () => {
            consumer = createConsumer()
            streams = createStreams(consumer)
        })

        t.afterEach(async () => {
            if (consumer) consumer.disconnect()
        })

        await t.test('returns a stream', async (t) => {
            const testTopic = `topic-${secureRandom()}`
            const streamA = streams.stream({ topic: testTopic, partition: 0 })
            
            t.ok(streamA)
            
            const streamB = streams.stream({ topic: testTopic, partition: 1 })
            const streamC = streams.stream({ topic: `another-topic`, partition: 0 })

            t.ok(streamA !== streamB && streamA !== streamC, 'returns a new stream for every distinct topic-partition')

            const streamD = streams.stream({ topic: testTopic, partition: 0 })
            t.equal(streamA, streamD, 'returns the same stream for calls with identical topic partition')
        })        
    })

    await t.test('TaskStreams.start', async (t) => {
        let consumer, streams, testTopic
        t.beforeEach(async () => {
            consumer = createConsumer()
            streams = createStreams(consumer)
            testTopic = `topic-${secureRandom()}`
            await createTopic({ topic: testTopic, partitions: 2 })
        })

        t.afterEach(async () => {
            if (consumer) consumer.disconnect()
            if (testTopic) await deleteTopic(testTopic)
        })

        await t.test('runs the consumer by injecting messages into streams', async (t) => {
            const testMessages = Array(20)
                .fill({})
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                })
            
            await produceMessages(testTopic, testMessages)

            await consumer.connect()
            await consumer.subscribe({ topic: testTopic, fromBeginning: true })
            await streams.start()

            const stream = streams.stream({ topic: testTopic, partition: 0 })

            const consumedMessages = await H(stream).take(testMessages.length).collect().toPromise(Promise)

            t.deepEqual(
                consumedMessages.map(({ key, value }) => {
                    return { key: key.toString(), value: value.toString() }
                }),
                testMessages.map(({ key, value }) => ({ key, value }))
            , 'injects messages consumed into the corresponding stream')
        })

        await t.test('can consume message with a stream providing back-pressure', async (t) => {
            const pauseSpy = spy(consumer, 'pause')
            const resumeSpy = spy(consumer, 'resume')
            
            const testMessages = Array(40)
                .fill({})
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                })

            await produceMessages(testTopic, testMessages)

            await consumer.connect()
            await consumer.subscribe({ topic: testTopic, fromBeginning: true })
            await streams.start()

            const stream = streams.stream({ topic: testTopic, partition: 0 })

            await H(stream)
                .ratelimit(Math.ceil(testMessages.length / 10), 10)
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            t.ok(pauseSpy.called, 'pauses consumption of topic to deal with back pressure')
            t.ok(resumeSpy.called, 'resumes consumption of topic to deal with back pressure')

        })

        await t.test('will stop injecting messages into streams when consumer stopped running and destroy the stream', async (t) => {
            const testMessages = Array(100)
                .fill({})
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                })

            await produceMessages(testTopic, testMessages)

            await consumer.connect()
            await consumer.subscribe({ topic: testTopic, fromBeginning: true })
            await streams.start()

            var stopped = false

            const stream = streams.stream({ topic: testTopic, partition: 0 })

            const consumedMessages = await H(stream)
                .ratelimit(Math.ceil(testMessages.length / 10), 10)
                .tap(() => {
                    if (!stopped) {
                        consumer.stop()
                        stopped = true
                    }
                })
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            t.ok(consumedMessages.length < testMessages.length, 'stops injecting messages into the stream once stopped')
            t.ok(stream.destroyed, 'destroys streams when consumer stops')
        })

        await t.test('will stop injecting messages into stream when stream ends while messages still being consumed', async (t) => {
            const testMessages = Array(100)
                .fill({})
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                })

            await produceMessages(testTopic, testMessages)

            await consumer.connect()
            await consumer.subscribe({ topic: testTopic, fromBeginning: true })
            await streams.start()

            var messageCount = 0

            const stream = streams.stream({ topic: testTopic, partition: 0 })

            const consumedMessages = await H(stream)
                .ratelimit(1, 100)
                .tap(() => {
                    messageCount++
                    if (messageCount === 3) {
                        stream.end()
                    }
                })
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            t.ok(consumedMessages.length < testMessages.length, 'stops injecting messages into the stream once stopped')
            t.ok(stream.destroyed, 'stream is destroyed after it ends')
        })

        await t.test('will keep fetching and injecting messages for fast topics in the presence of slower topics', async (t) => {
            const testMessages = Array(20)
                .fill({})
                .map((obj, n) => {
                    const value = secureRandom()
                    // alternate between 2 partitions
                    return { key: `key-${value}`, value: `value-${value}`, partition: n % 2 }
                })

            await produceMessages(testTopic, testMessages)

            await consumer.connect()
            await consumer.subscribe({ topic: testTopic, fromBeginning: true })
            await streams.start()

            var messageCount = 0

            const stream = streams.stream({ topic: testTopic, partition: 0 })

            const fastStream = streams.stream({ topic: testTopic, partition: 0 });
            const slowStream = streams.stream({ topic: testTopic, partition: 1 });


            const consumedMessages = await H([
                    { stream: fastStream, timeout: 1 },
                    { stream: slowStream, timeout: 50 }
                ])
                .map(({ stream, timeout }) => {
                    return H(stream).ratelimit(1, timeout)
                })
                .merge()
                .take(testMessages.length)
                .drop(2) // first two messages will be concurrent, so undefined order
                .collect()
                .toPromise(Promise)

            const consumedPartitions = consumedMessages.map((m : Message) => m.partition)

            t.deepEqual(consumedPartitions, [
                ...Array((testMessages.length - 2) / 2).fill(0),
                ...Array((testMessages.length - 2) / 2).fill(1)
            ], 'fetches and inject messages for faster partitions as slower partitions experience back-pressure');
        })
    })
})
