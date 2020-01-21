import Tap from 'tap'
import createLocalAssignmentContext from '../../src/assignment-contexts/local'
import { spy } from 'sinon'
import { ProcessingContext } from '../../src/processors'

import { secureRandom } from '../helpers'

Tap.test('Processor pipeline', async (t) => {
    const testAssignment = {
        topic: 'test-topic',
        partition: 0,
        group: 'test-group'
    }

    async function testProcessor (processors, assignment = testAssignment, initialState = {}) {
        processors = [].concat(processors) // one or more processors
        
        return await createLocalAssignmentContext({
            assignment,
            processors: processors.map((processor) => async (assignment) => processor),
            initialState
        })
    }

    await t.test('pipes messages and subsequent results to processors in order', async (t) => {
        const testMessages = Array(2).fill({}).map(() => ({
            topic: testAssignment.topic,
            partition: testAssignment.partition,
            value: `value-${secureRandom()}`,
            key: `value-${secureRandom()}`
        }))

        const processMessageOne = spy(({ key, value }) => {
            return {
                key: JSON.parse(key.toString()),
                value: JSON.parse(value.toString())
            }
        })

        const processMessageTwo = spy(({ key, value }) => {
            return {
                key: `processed-${key}`,
                value: `processed-${value}`
            }
        })

        const testContext = await testProcessor([
            processMessageOne,
            processMessageTwo
        ])

        const injectedMessages = testMessages.map((msg) => testContext.inject(msg))

        await testContext.caughtUp()

        t.equivalent(
            testContext.processingResults,
            testMessages.map(({ key, value }) => ({
                key: `processed-${key}`,
                value: `processed-${value}`
            }))
        , 'messages can be transformed into results, with results of preceding processors forwarded as input to subsequent processors')
        

        t.equal(
            processMessageOne.secondCall.calledAfter(processMessageTwo.firstCall), 
            true,
            'processing of messages happens depth-first, with single messages going through entire pipeline before accepting next message'
        )
    })

    await t.test('provides a processing context', async (t) => {
        const testMessage = {
            topic: testAssignment.topic,
            partition: testAssignment.partition,
            value: `value-${secureRandom()}`,
            key: `value-${secureRandom()}`
        }

        const testContext = await testProcessor([
            (message, context: ProcessingContext) => {
                return {
                    topic: context.topic(),
                    partition: context.partition(),
                    group: context.group(),
                    offset: context.offset(),
                    timestamp: context.timestamp()
                }
            },
            (contextDescription, context: ProcessingContext) => {
                const newDescription = {
                    topic: context.topic(),
                    partition: context.partition(),
                    group: context.group(),
                    offset: context.offset(),
                    timestamp: context.timestamp()
                }

                t.equivalent(contextDescription, newDescription, 'context is the same between processors')

                return newDescription
            }
        ])

        const injectedMessage = testContext.inject(testMessage)

        await testContext.caughtUp()

        t.equal(testContext.processingResults.length, 1)
        t.match(
            testContext.processingResults[0],
            {
                topic: testAssignment.topic,
                partition: testAssignment.partition,
                group: testAssignment.group
            }
        , 'context contains functions returning the topic, partition, group of the assignment')

        t.match(
            testContext.processingResults[0],
            {
                offset: injectedMessage.offset,
                timestamp: injectedMessage.timestamp
            }
        , 'context contains functions returning the offset and timestamp of the message being consumed')
    })

    await t.test('context.abandon', async (t) => {
        const testMessages = Array(3).fill({}).map(() => ({
            topic: testAssignment.topic,
            partition: testAssignment.partition,
            value: `value-${secureRandom()}`,
            key: `value-${secureRandom()}`
        }))

        const processMessageOne = spy(({ offset }, context) => {
            if (offset === '1') {
                return context.abandon
            } else {
                return offset
            }
        })

        const processMessageTwo = spy((offset) => {
            return offset
        })

        const testContext = await testProcessor([ processMessageOne, processMessageTwo ])

        const injectedMessages = testMessages.map(testContext.inject)
        await testContext.caughtUp()

        t.equal(testContext.processingResults.length, testMessages.length - 1)
        t.equivalent(
            testContext.processingResults,
            [injectedMessages[0].offset, injectedMessages[2].offset]
        )
        t.ok(processMessageTwo.calledTwice, 'abandoned messages are not passed to downstream message processors')
    })
})