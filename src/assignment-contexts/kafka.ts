import H from 'highland'

export default async function createContext ({
    assignment,
    processors,
    messagesStream
}: {
    assignment: any,
    processors: any[],
    messagesStream: Highland.Stream<any>
}) : Promise<any> {
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

    const processedMessages = await processors.reduce(async (s, setupProcessor) => {
        const stream = await s

        const messageProcessor = await setupProcessor(processorContext)

        return stream.map(async (message) => await messageProcessor(message))
            .flatMap((awaitingProcessing) => H(awaitingProcessing))
    }, Promise.resolve(messagesStream))

    return {
        stream: processedMessages,
        async stop() {}
    }
}