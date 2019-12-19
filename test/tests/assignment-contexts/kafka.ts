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
})