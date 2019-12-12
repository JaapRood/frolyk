import Tap from 'tap'
import { Kafka } from 'kafkajs'
import Config from './config'
import createStreams from '../src/streams'
import Uuid from 'uuid/v4'
import Crypto from 'crypto'

const secureRandom = (length = 10) =>
    `${Crypto.randomBytes(length).toString('hex')}-${process.pid}-${Uuid()}`

const createConsumer = (options = {}) => {
    const kafka = new Kafka({
        brokers: Config.kafka.brokers
    })
    
    return kafka.consumer({
        groupId: `group-${secureRandom()}`,
        ...options,
        maxWaitTimeInMs: 100
    })
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
})
