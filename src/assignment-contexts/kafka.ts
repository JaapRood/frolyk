import H from 'highland'
import { TopicPartitionStream, Message } from '../streams'
import { Admin, Consumer } from 'kafkajs'
import Long from 'long'
import Invariant from 'invariant'

export default async function createContext ({
    assignment,
    processors,
    stream: rawStream,
    admin,
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
    consumer: Consumer
}) : Promise<{
    topic: string,
    partition: number,
    stream: Highland.Stream<Message>,
    start () : Promise<any>,
    stop () : Promise<any>
}> {
    const controlledStream = H(rawStream)

    /* istanbul ignore next */
    const fetchWatermarks = async () => {
        const offsets = await admin.fetchTopicOffsets(assignment.topic)
        const partitionOffset = offsets.find((offset) => assignment.partition === offset.partition)

        return {
            high: partitionOffset.high && Long.fromString(partitionOffset.high),
            low: partitionOffset.low && Long.fromString(partitionOffset.low)
        }
    }

    const processorContext = {
        async caughtUp(offset: string | Long) {
            var offsetLong : Long
            try {
                offsetLong = Long.fromValue(offset)
            } catch (parseError) {
                Invariant(false, 'Valid offset (parseable as Long) is required to verify the assignment is caught up at it')
            }

            const watermarks = await fetchWatermarks()

            return watermarks.high.lt(1) || offsetLong.gte(watermarks.high)
        },
        
        async commitOffset(newOffset: string | Long, metadata: string | null = null) {
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
        
        /* istanbul ignore next */
        async committed() {},
        /* istanbul ignore next */
        async isEmpty() {},
        /* istanbul ignore next */
        async log() {},
        /* istanbul ignore next */
        async seek() {},
        /* istanbul ignore next */
        async send() { },
        /* istanbul ignore next */
        async watermarks() { },

        topic: assignment.topic,
        partition: assignment.partition,
        group: assignment.group
    }

    const processedStream = await processors.reduce(async (s, setupProcessor) => {
        const stream = await s

        const messageProcessor = await setupProcessor(processorContext)

        return stream.map(async (message) => await messageProcessor(message))
            .flatMap((awaitingProcessing) => H(awaitingProcessing))
    }, Promise.resolve(controlledStream))

    return {
        topic: assignment.topic,
        partition: assignment.partition,

        stream: processedStream,
        
        /* istanbul ignore next */
        async start() {},
        /* istanbul ignore next */
        async stop() {}
    }
}