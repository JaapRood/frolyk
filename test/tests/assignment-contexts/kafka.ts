import Tap from 'tap'
import H from 'highland'
import createStreams, { Message } from '../../../src/streams'
import createKafkaAssignmentContext from '../../../src//assignment-contexts/kafka'
import { spy } from 'sinon'

import {
    secureRandom,
    createConsumer,
    createTopic,
    deleteTopic,
    produceMessages
} from '../../helpers'

Tap.test('AssignmentContext.Kafka', async (t) => {
    await t.test('can be created', async (t) => {
        const testTopic = `topic-${secureRandom()}`
        const testGroup = `group-${secureRandom()}`
        const consumer = createConsumer()
        const streams = createStreams(consumer)
        const stream = streams.stream({ topic: testTopic, partition: 0 })
        
        const context = await createKafkaAssignmentContext({
            assignment: { topic: testTopic, partition: 0, group: testGroup },
            consumer,
            processors: [],
            stream
        })
    })

    await t.test('processing pipeline', async (t) => {
        let testAssignment, consumer, streams, stream
        
        t.beforeEach(async () => {
            testAssignment = {
                topic: `topic-${secureRandom()}`,
                partition: 0,
                group: `group-${secureRandom()}`
            }
            consumer = createConsumer({ groupId: testAssignment.group })
            streams = createStreams(consumer)
            stream = streams.stream({ topic: testAssignment.topic, partition: 0 })
            await consumer.connect()
            await consumer.subscribe({ topic: testAssignment.topic })
            await streams.start()
        })

        t.afterEach(async () => {
            if (consumer) await consumer.disconnect()
        })

        const testProcessor = (setupProcessors, assignment = testAssignment) => {
            setupProcessors = [].concat(setupProcessors) // one or more processors
            
            return createKafkaAssignmentContext({
                assignment,
                consumer,
                processors: setupProcessors,
                stream
            })
        }

        await t.test('returns a stream with all processors applied in order', async (t) => {
            const testMessages = Array(100).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            await produceMessages(testAssignment.topic, testMessages)
            
            const processMessageOne = spy(({ key, value }) => ({ 
                key: key.toString(), 
                value: value.toString() 
            }))

            const processMessageTwo = spy(( { key, value }) => ({
                key: `processed-${key}`,
                value: `processed-${value}`
            }))
            
            const context = await testProcessor([
                async (assignment) => {
                    return processMessageOne
                },
                async (assignment) => {
                    return processMessageTwo
                }
            ])

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            t.equal(processMessageOne.callCount, testMessages.length)
            t.equal(processMessageTwo.callCount, testMessages.length)
            t.deepEqual(
                processingResults,
                testMessages.map(({ key, value }) => ({ 
                    key: `processed-${key}`, 
                    value: `processed-${value}`
                }))
            , 'applies processors to stream of messages')
        })

    })
})