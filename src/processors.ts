import H from 'highland'
import { AssignmentContext } from './assignment-contexts/index'
import { Message } from './streams'

interface ProcessorSetup {
    (assignment: any) : ProcessorFunction
    (assignment: any) : ProcessorFunction[]
}

interface ProcessingContext {}

interface ProcessorFunction {
    (val: any, context: ProcessingContext) : Promise<any>
    (val: any, context: ProcessingContext) : any
}

export async function createPipeline(
    assignmentContext : AssignmentContext, 
    processors : ProcessorSetup[]
): Promise<(controlledStream: Highland.Stream<Message>) => Highland.Stream<any>> {
    const messageProcessors = await processors.reduce(async (p, setupProcessor) => {
        const processors = await p

        const messageProcessor = await setupProcessor(assignmentContext)

        return [...processors, messageProcessor]
    }, Promise.resolve([]))
    
    return (controlledStream) => {    
        const processedStream = messageProcessors.reduce((stream, messageProcessor) => {
            return stream
                .map(async (message) => await messageProcessor(message, {}))
                .flatMap((awaitingProcessing) => H(awaitingProcessing))
        }, controlledStream)

        return processedStream
    }
}