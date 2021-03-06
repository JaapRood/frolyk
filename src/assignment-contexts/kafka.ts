import H from 'highland'
import { Admin, Consumer, Producer, CompressionTypes } from 'kafkajs'
import Long from 'long'
import Invariant from 'invariant'
import _groupBy from 'lodash.groupby'

import { TopicPartitionStream, Message } from '../streams'
import { LogicalOffset, LogicalLiteralOffset, isEarliest, isLatest } from '../offsets'
import { AssignmentContext } from './index'
import { createPipeline } from '../processors'

export default async function createContext ({
    assignment,
    processors,
    stream: rawStream,
    admin,
    createProducer,
    consumer
}: {
    assignment: {
        topic: string,
        partition: number,
        group: string
    },
    processors: any[],
    stream: TopicPartitionStream,
    admin: Admin,
    createProducer: () => Producer,
    consumer: Consumer
}) : Promise<{
    topic: string,
    partition: number,
    stream: Highland.Stream<Message>,
    start () : Promise<any>,
    stop () : Promise<any>
}> {
    const controlledStream = H(rawStream)
    const producer = createProducer()

    /* istanbul ignore next */
    const fetchWatermarks = async () => {
        const offsets = await admin.fetchTopicOffsets(assignment.topic)
        const partitionOffset = offsets.find((offset) => assignment.partition === offset.partition)

        return {
            high: partitionOffset.high && Long.fromString(partitionOffset.high),
            low: partitionOffset.low && Long.fromString(partitionOffset.low)
        }
    }

    const assignmentContext: AssignmentContext = {
        async caughtUp(offset) {
            var offsetLong : Long
            try {
                offsetLong = Long.fromValue(offset)
            } catch (parseError) {
                Invariant(false, 'Valid offset (parseable as Long) is required to verify the assignment is caught up at it')
            }

            const watermarks = await fetchWatermarks()

            return watermarks.high.lt(1) || offsetLong.gte(watermarks.high)
        },
        
        async commitOffset(newOffset, metadata = null) {
            try {
                newOffset = Long.fromValue(newOffset)
            } catch (parseError) {
                Invariant(false, 'Valid offset (parseable as Long) is required to commit offset for assignment')
            }
            
            await consumer.commitOffsets([{ 
                topic: assignment.topic,
                partition: assignment.partition,
                offset: newOffset.toString(),
                metadata
            }])
        },
        
        async committed() {
            const payload = { groupId: assignment.group, topic: assignment.topic }

            const partitionOffsets = await admin.fetchOffsets(payload)
            const { offset, metadata } = partitionOffsets.find(({ partition }) => partition === assignment.partition)

            return { offset, metadata }
        },
        
        async isEmpty() {
            const watermarks = await fetchWatermarks()

            return watermarks.high.subtract(watermarks.low).lte(0)
        },

        /* istanbul ignore next */
        async log(tags, payload) {},
        
        async seek(offset: string | Long | LogicalOffset | LogicalLiteralOffset) {
            if (isEarliest(offset)) offset = (await fetchWatermarks()).low
            if (isLatest(offset)) offset = (await fetchWatermarks()).high

            return rawStream.seek(offset as string | Long)
        },

        async send(messages) {
            if (!Array.isArray(messages)) messages = [messages]
            const messagesByTopic = _groupBy(messages, (message) => message.topic)
            const topics = Object.keys(messagesByTopic)
            const topicMessages = topics.reduce((topicMessages, topic) => {
                topicMessages.push({
                    topic,
                    messages: messagesByTopic[topic]
                })
                return topicMessages
            }, [])

            // TODO: incorporate acks, timeouts and compression into API somehow
            return producer.sendBatch({
                topicMessages, 
                acks: -1,
                timeout: 30 * 1000,
                compression: CompressionTypes.None
            })
        },
        /* istanbul ignore next */
        async watermarks() {
            const { high, low } = await fetchWatermarks()
            
            return {
                highOffset: high.toString(),
                lowOffset: low.toString()
            }
        },

        topic: assignment.topic,
        partition: assignment.partition,
        group: assignment.group
    }

    const [processingPipeline, processedOffsets] = await createPipeline(assignmentContext, processors)
    const processedStream = controlledStream.through(processingPipeline)

    processedOffsets.each((offset) => {
        // this stream is mostly used for testing, so here we'll just want to make sure it doesn't
        // become a memory leak by instantly relieving all back-pressure
    })

    return {
        topic: assignment.topic,
        partition: assignment.partition,

        stream: processedStream,
        
        async start() {
            await producer.connect()
        },
        async stop() {
            await producer.disconnect()
        }
    }
}