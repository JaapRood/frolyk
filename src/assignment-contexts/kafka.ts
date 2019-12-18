import H from 'highland'
import { TopicPartitionStream, Message } from '../streams'

export default async function createContext ({
    assignment,
    processors,
    stream: rawStream
}: {
    assignment: any,
    processors: any[],
    stream: TopicPartitionStream
}) : Promise<{
    topic: string,
    partition: number,
    stream: Highland.Stream<Message>,
    start () : Promise<any>,
    stop () : Promise<any>
}> {
    const controlledStream = H(rawStream)

    const processorContext = {
        /* istanbul ignore next */
        async caughtUp(offset) {},
        /* istanbul ignore next */
        async commitOffset() {},
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
        async start() {},
        async stop() {}
    }
}