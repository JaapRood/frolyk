import Tap from 'tap'
import H from 'highland'
import Long from 'long'
import createStreams, { Message } from '../../../src/streams'
import createKafkaAssignmentContext from '../../../src/assignment-contexts/kafka'
import { logLevel as LOG_LEVEL, CompressionTypes } from 'kafkajs'
import { OffsetAndMetadata, Watermarks } from '../../../src/assignment-contexts/index'
import { spy } from 'sinon'

import {
    secureRandom,
    createAdmin,
    createConsumer,
    createProducer,
    createTopic,
    deleteTopic,
    fetchOffset,
    produceMessages
} from '../../helpers'
import { start } from 'repl'

const setupAssignmentTests = (t, autoStart = true) => {
    let testAssignment, admin, consumer, streams, contexts, topics

    const testTopic = (partitions = 1) => {
        partitions = Math.max(1, partitions)
        const uniqueId = secureRandom()
        const topic = {
            topic: `topic-${uniqueId}`,
            partitions
        }
        
        topics.push(topic)

        return topic
    }
    
    const testProcessor = async (setupProcessors, assignment = testAssignment) => {
        setupProcessors = [].concat(setupProcessors) // one or more processors


        const context = await createKafkaAssignmentContext({
            assignment,
            admin,
            consumer,
            createProducer: () => createProducer({ logLevel: LOG_LEVEL.ERROR }),
            processors: setupProcessors,
            stream: streams.stream({ topic: assignment.topic, partition: assignment.partition })
        })

        contexts.push(context)

        await context.start()

        return context
    }

    const start = async () => {
        for (let { topic, partitions } of topics) {
            await createTopic({ topic, partitions })
            await consumer.subscribe({ topic, fromBeginning: true })
        }
        await admin.connect()
        await consumer.connect()
        await streams.start()
    }

    t.beforeEach(async () => {
        contexts = []
        topics = []
        let defaultTopic = testTopic()
        testAssignment = {
            topic: defaultTopic.topic,
            partition: 0,
            group: `group-${secureRandom()}`
        }
        admin = createAdmin({ logLevel: LOG_LEVEL.ERROR })
        consumer = createConsumer({ groupId: testAssignment.group, logLevel: LOG_LEVEL.ERROR })
        streams = createStreams(consumer)

        if (autoStart) {
            await start()
        }
    })

    t.afterEach(async () => {
        if (consumer) await consumer.disconnect()
        if (admin) await admin.disconnect()
        for (let context of contexts) {
            await context.stop()
        }
        for (let { topic } of topics) {
            await deleteTopic(topic)
        }
    })

    return {
        testTopic,
        testAssignment: () => testAssignment, 
        admin: () => admin, 
        consumer: () => consumer, 
        start,
        testProcessor
    }
}

Tap.test('AssignmentContext.Kafka', async (t) => {
    await t.test('can be created', async (t) => {
        const testTopic = `topic-${secureRandom()}`
        const testGroup = `group-${secureRandom()}`
        const admin = createAdmin()
        const consumer = createConsumer()
        const streams = createStreams(consumer)
        const stream = streams.stream({ topic: testTopic, partition: 0 })
        
        const context = await createKafkaAssignmentContext({
            assignment: { topic: testTopic, partition: 0, group: testGroup },
            admin,
            consumer,
            createProducer: () => createProducer({ logLevel: LOG_LEVEL.ERROR }),
            processors: [],
            stream
        })
    })

    await t.test('processing pipeline', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('returns a stream with all processors applied in order', async (t) => {
            const testMessages = Array(100).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            await produceMessages(testAssignment().topic, testMessages)
            
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

        await t.test('propagates errors in processors through the processing stream', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            await produceMessages(testAssignment().topic, testMessages)

            const context = await testProcessor([
                async (assignment) => (message) => {
                    throw new Error('uncaught-processor-error')
                }
            ])

            const processingResults = context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            await t.rejects(processingResults, /uncaught-processor-error/, 'testing rejection assertion')
        })
    })

    await t.test('assignment.commitOffset', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('can commit an offset to broker while processing messages', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))
            await produceMessages(testAssignment().topic, testMessages)

            const committedOffsets = H()

            const context = await testProcessor([
                async (assignment) => async (message) => {
                    await assignment.commitOffset(Long.fromValue(message.offset).add(1))
                    committedOffsets.write(await fetchOffset({ topic: testAssignment().topic, partition: 0, groupId: testAssignment().group }))
                }
            ])
            
            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            const committed = await committedOffsets.take(testMessages.length).collect().toPromise(Promise)

            t.equivalent(
                committed.map(({ offset }) => offset.toString()),
                testMessages.map((message, i) => `${i + 1}`)
            )
        })

        await t.test('requires string offsets to be parseable as Long', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const context = await testProcessor([
                async (assignment) => async (message) => {
                    await assignment.commitOffset('not-a-valid-offset')
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)
            
            const processing = context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            await t.rejects(processing, /Valid offset/, 'throws an error requiring a valid offset')
        })

        await t.test('can commit an offset with metadata to broker while processing messages', async () => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))
            await produceMessages(testAssignment().topic, testMessages)

            const committedOffsets = H()
            const context = await testProcessor([
                async (assignment) => async (message) => {
                    await assignment.commitOffset(Long.fromValue(message.offset).add(1), message.value.toString('utf-8'))
                    committedOffsets.write(await fetchOffset({ topic: testAssignment().topic, partition: 0, groupId: testAssignment().group }))
                }
            ])

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            const committed = await committedOffsets.take(testMessages.length).collect().toPromise(Promise)

            t.equivalent(
                committed.map(({ metadata }) => metadata),
                testMessages.map(({ value }, i) => value)
            )
        })

        await t.test('can commit an offset to broker while setting up assignment', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))
            await produceMessages(testAssignment().topic, testMessages)

            const committedOffsets = H()

            const context = await testProcessor([
                async (assignment) => {
                    await assignment.commitOffset(Long.fromNumber(testMessages.length / 2))
                    committedOffsets.write(await fetchOffset({ topic: testAssignment().topic, partition: 0, groupId: testAssignment().group }))

                    return (message) => message
                }
            ])

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            committedOffsets.end()
            const committed : any[] = await committedOffsets.collect().toPromise(Promise)

            t.equal(committed.length, 1)
            t.equal(committed[0].offset.toString(), `${testMessages.length / 2}`, )
        })
    })

    await t.test('assignment.caughtUp', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('can query whether the assignment has caught up to the end of the log given an offset', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const caughtUpResults = H()

            const context = await testProcessor([
                async (assignment) => {
                    caughtUpResults.write(await assignment.caughtUp('0'))

                    return async (message) => {
                        caughtUpResults.write(await assignment.caughtUp(Long.fromValue(message.offset).add(1)))
                    }
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            caughtUpResults.end()
            const results = await caughtUpResults.collect().toPromise(Promise)
            t.equal(results.length, testMessages.length + 1)

            const [setupResult, ...processResults] = results

            t.equal(setupResult, true, 'returns true for empty logs')
            t.equivalent(
                processResults,
                testMessages.map((message, i) => i + 1 === testMessages.length)
            , 'returns true when passed offset is at the highwater mark of the partition')
        })

        await t.test('requires string offsets to be parseable as Long', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const context = await testProcessor([
                async (assignment) => async (message) => {
                    await assignment.caughtUp('not-a-valid-offset')
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processing = context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            await t.rejects(processing, /Valid offset/, 'throws an error requiring a valid offset')
        })
    })

    await t.test('assignment.committed', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('can query the last committed offset for the consumer', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const committedResults : Highland.Stream<OffsetAndMetadata> = H()

            const context = await testProcessor([
                async (assignment) => {
                    committedResults.write(await assignment.committed())

                    return async (message) => {
                        await assignment.commitOffset(Long.fromValue(message.offset).add(1))
                        committedResults.write(await assignment.committed())
                    }
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            committedResults.end()
            const results = await committedResults.collect().toPromise(Promise)
            t.equal(results.length, testMessages.length + 1)

            const [setupResult, ...processResults] = results

            t.equal(setupResult.offset, '-1', 'returns -1 when no commit for partition was made with consumer group')

            t.equivalent(
                processResults.map(({ offset }) => offset),
                testMessages.map((message, i) => `${i + 1}`)
                , 'returns current offset when consumer has offset committed for partition')
        })

        await t.test('can query the last committed offset with metadata commit for the consumer', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const committedResults: Highland.Stream<OffsetAndMetadata> = H()

            const context = await testProcessor([
                async (assignment) => {
                    return async (message) => {
                        await assignment.commitOffset(Long.fromValue(message.offset).add(1), message.value.toString('utf-8'))
                        committedResults.write(await assignment.committed())
                    }
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            committedResults.end()
            const results = await committedResults.collect().toPromise(Promise)
            t.equal(results.length, testMessages.length)

            t.equivalent(
                results.map(({ metadata }) => metadata),
                testMessages.map((message, i) => message.value)
                , 'returns metatdata committed with offsets ')
        })
    })

    await t.test('assignment.isEmpty', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('can query whether partition of assignment is empty', async (t) => {
            const testMessages = Array(10).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const isEmpyResults: Highland.Stream<OffsetAndMetadata> = H()

            const context = await testProcessor([
                async (assignment) => {
                    isEmpyResults.write(await assignment.isEmpty())

                    return async (message) => {
                        isEmpyResults.write(await assignment.isEmpty())
                    }
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            isEmpyResults.end()
            const results = await isEmpyResults.collect().toPromise(Promise)
            t.equal(results.length, testMessages.length + 1)

            const [setupResult, ...processResults] = results

            t.equal(setupResult, true, 'returns true when partition contains no messages')

            t.equivalent(
                processResults,
                testMessages.map(() => false)
                , 'returns false when partitions contains messages')
        })
    })

    await t.test('assignment.send', async (t) => {
        const { testAssignment: inputAssignment, testProcessor, testTopic, start } = setupAssignmentTests(t, false)
        var outputAssignment

        t.beforeEach(async () => {
            let outputTopic = testTopic(1)
            outputAssignment = {
                topic: outputTopic.topic,
                partition: 0,
                group: inputAssignment().group
            }

            await start()
        })

        await t.test('can produce messages to a topic during message processing', async (t) => {
            const testMessages = Array(15).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const inputContext = await testProcessor([
                async (assignment) => async (message) => {
                    await assignment.send([{
                        topic: outputAssignment.topic,
                        partition: outputAssignment.partition,
                        key: message.key,
                        value: message.value
                    }])
                }
            ], inputAssignment())

            const outputContext = await testProcessor([
                async (assignment) => async (message) => {
                    return {
                        key: message.key.toString('utf-8'),
                        value: message.value.toString('utf-8')
                    }
                }
            ], outputAssignment)

            await produceMessages(inputAssignment().topic, testMessages)

            const processingResults = await Promise.all([
                inputContext.stream
                    .take(testMessages.length)
                    .collect()
                    .toPromise(Promise),
                outputContext.stream
                    .take(testMessages.length)
                    .collect()
                    .toPromise(Promise)
            ])
            
            const producedMessages = processingResults[1]

            const inputRecords = testMessages.map(({ key, value }) => ({ key, value }))
            const outputRecords = producedMessages.map(({ key, value }) => ({ key, value }))

            t.equivalent(inputRecords, outputRecords)
        })

        await t.test('can produce messages to a topic during processor setup', async (t) => {
            const testMessages = Array(15).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const context = await testProcessor([
                async (assignment) => {
                    await assignment.send(testMessages.map((msg) => ({
                        ...msg,
                        topic: outputAssignment.topic
                    })))

                    return async (message) => {
                        return {
                            key: message.key.toString('utf-8'),
                            value: message.value.toString('utf-8')
                        }
                    }
                }

            ], outputAssignment)

            await produceMessages(inputAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            const inputRecords = testMessages.map(({ key, value }) => ({ key, value }))
            const outputRecords = processingResults.map(({ key, value }) => ({ key, value }))

            t.equivalent(inputRecords, outputRecords)
        })
    })

    await t.test('assignment.watermarks', async (t) => {
        const { testAssignment, testProcessor } = setupAssignmentTests(t)

        await t.test('can query whether partition of assignment is empty', async (t) => {
            const testMessages = Array(15).fill({}).map(() => ({
                value: `value-${secureRandom()}`,
                key: `value-${secureRandom()}`,
                partition: 0
            }))

            const watermarksResults: Highland.Stream<Watermarks> = H()

            const context = await testProcessor([
                async (assignment) => {
                    watermarksResults.write(await assignment.watermarks())

                    return async (message) => {
                        watermarksResults.write(await assignment.watermarks())
                    }
                }
            ])

            await produceMessages(testAssignment().topic, testMessages)

            const processingResults = await context.stream
                .take(testMessages.length)
                .collect()
                .toPromise(Promise)

            watermarksResults.end()
            const results = await watermarksResults.collect().toPromise(Promise)
            t.equal(results.length, testMessages.length + 1)

            const [setupResult, ...processResults] = results

            t.equivalent(
                setupResult, 
                { highOffset: '0', lowOffset: '0' }
            , 'returns high and low offsets of 0 for new and empty partitions')

            t.equivalent(
                processResults,
                testMessages.map(() => ({
                    highOffset: `${testMessages.length}`,
                    lowOffset: '0'
                }))
            , 'returns min and max offsets for partition of assignment')
        })
    })
})