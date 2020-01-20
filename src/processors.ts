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
    controlledStream: Highland.Stream<Message>,
    assignmentContext : AssignmentContext, 
    processors : ProcessorSetup[]
) : Promise<Highland.Stream<any>> {
    const processedStream = await processors.reduce(async (s, setupProcessor) => {
        const stream : Highland.Stream<any> = await s
        
        const messageProcessor = await setupProcessor(assignmentContext)

        return stream
            .map(async (message) => await messageProcessor(message, {}))
            .flatMap((awaitingProcessing) => H(awaitingProcessing))
    }, Promise.resolve(controlledStream))

    return processedStream
}