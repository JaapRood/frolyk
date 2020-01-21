import H from 'highland'
import { AssignmentContext } from './assignment-contexts/index'
import { Message } from './streams'

const abandon = Symbol('abandon')

interface ProcessorSetup {
    (assignment: any) : ProcessorFunction
    (assignment: any) : ProcessorFunction[]
}

export interface ProcessingContext {
    abandon,
    toString: () => string,
    commit: (metadata: any) => Promise<void>,
    group: () => string,
    offset: () => string,
    partition: () => number,
    topic: () => string,
    timestamp: () => string
}

interface ProcessorFunction {
    (val: any, context: ProcessingContext) : Promise<any>
    (val: any, context: ProcessingContext) : any
}


export async function createPipeline(
    assignmentContext : AssignmentContext, 
    processors : ProcessorSetup[]
): Promise<[
    (controlledStream: Highland.Stream<Message>) => Highland.Stream<any>,
    Highland.Stream<string>
]> {
    const messageProcessors : ProcessorFunction[] = await processors.reduce(async (p, setupProcessor) => {
        const processors = await p

        const messageProcessor = await setupProcessor(assignmentContext)

        return [...processors, messageProcessor]
    }, Promise.resolve([]))
    
    const processedOffsets : Highland.Stream<string> = H()

    const pipeline = (controlledStream) => {    
        const processedStream = controlledStream.consume(function (err, x, push, next) {
            if (err) {
                // forward errors
                push(err)
                next()
                return
            } else if (H.isNil(x)) {
                // forward end of stream
                processedOffsets.end()
                push(null, x)
                return 
            }

            const message = (x as Message)
            const { highWaterOffset, offset, partition, topic, timestamp } = message

            const context = {
                abandon,
                toString: () => `processor context (o=${offset} p=${partition} t=${topic}, ho=${highWaterOffset})`,
                commit: (metadata) => assignmentContext.commitOffset(offset, metadata),
                group: () => assignmentContext.group,
                offset: () => offset,
                partition: () => partition,
                topic: () => topic,
                timestamp: () => timestamp
            }

            const processingMessage = messageProcessors.reduce(async (r, messageProcessor) => {
                const prevResult : any = await r
                if (prevResult === abandon) return prevResult

                const result = await messageProcessor(prevResult, context)
                
                return result
            }, Promise.resolve(message))

            processingMessage.then((result) => {
                if (result === abandon) return
                push(null, result)
                processedOffsets.write(offset)
                next()
            }, (err) => {
                push(err)
                next()
            })
        })

        return processedStream
    }

    return [pipeline, processedOffsets]
}