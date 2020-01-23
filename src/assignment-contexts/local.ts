import H from 'highland'
import Long from 'long'

import { AssignmentContext, OffsetAndMetadata, NewMessage, ProducedMessageMetadata, Watermarks } from './index'
import { LogicalOffset, LogicalLiteralOffset, isEarliest, isLatest } from '../offsets'
import { createPipeline } from '../processors'
import { Message } from '../streams'

export interface AssignmentTestInterface {
	inject(payload: InjectMessagePayload): Message,
	inject(payload: Error): Error,
	committedOffsets: OffsetAndMetadata[],
	initialMessages: Message[],
	caughtUp() : Promise<void>,
	end(): Promise<void>,
	processing: Promise<void>,
	processingResults: any[],
	processedOffsets: string[],
	producedMessages: any[]
}

interface InternalMessage {
	topic: string,
	partition: number,
	value: Buffer | null,
	key: Buffer | null,
	offset: Long,
	timestamp: string
}

interface InjectMessagePayload {
	topic?: string,
	partition?: number,
	value?: any,
	key?: any,
	offset?: string,
	timestamp?: string
}

const createContext = async function({
	assignment,
	processors,
	initialState
}:{ 
	assignment: any,
	processors: any, 
	initialState?: any
}) : Promise<AssignmentTestInterface> {

	initialState = {
		lowOffset: 0,
		messages: [],
		...(initialState || {})
	}

	let producedOffset : number = initialState.lowOffset - 1
	let consumedOffset : number = initialState.lowOffset - 1
	let seekToOffset : number = -1
	let committedOffset : OffsetAndMetadata = { offset: '-1', metadata: null }
	const stream : Highland.Stream<InternalMessage | Error> = H()
	const committedOffsets : OffsetAndMetadata[] = []
	const injectedMessages : InternalMessage[] = []
	const producedMessages = []

	const createMessage = (payload: InjectMessagePayload): InternalMessage => {
		const value = !payload.value ? null :
			Buffer.isBuffer(payload.value) ? payload.value :
				Buffer.from(JSON.stringify(payload.value))

		const key = !payload.key ? null :
			Buffer.isBuffer(payload.key) ? payload.key :
				Buffer.from(JSON.stringify(payload.key))

		const offset = payload.offset && Long.fromValue(payload.offset) || Long.fromNumber(producedOffset + 1)

		if (offset.lte(producedOffset)) {
			throw new Error('Offset of injected message must be at or higher than the current highwatermark')
		}

		producedOffset = offset.toNumber()

		return {
			partition: assignment.partition,
			timestamp: `${Date.now()}`,
			topic: assignment.topic,
			...payload,
			value,
			key,
			offset
		}
	}

	const injectMessage = (message : InternalMessage) : InternalMessage => {
		injectedMessages.push(message)

		return writeMessageToStream(message)
	}

	const injectError = (error: Error) => {
		return stream.write(error)
	}

	const writeMessageToStream = (message : InternalMessage) : InternalMessage => {

		stream.write(message)

		return message
	}

	const highOffset = () : Long => {
		const lastMessage = injectedMessages[injectedMessages.length - 1]
		return lastMessage ? lastMessage.offset.add(1) : Long.fromNumber(initialState.lowOffset)
	}

	const lowOffset = () : Long => {
		const firstMessage = injectedMessages[0]
		return firstMessage ? firstMessage.offset : Long.fromNumber(initialState.lowOffset)
	}

	const assignmentContext: AssignmentContext= {
		async caughtUp(offset) {
			// TODO: deal with logical offsets
			return Long.fromValue(offset).add(1) >= highOffset()
		},
		
		async commitOffset(newOffset : string | Long, metadata : string | null = null) {
			newOffset = Long.fromValue(newOffset)

			if (newOffset.lte(-1)) {
				throw new Error('Offset must be a valid absolute offset to commit it')
			}

			const offset = { offset: newOffset.toString(), metadata }

			committedOffset = offset
			committedOffsets.push(offset)

			return Promise.resolve()
		},

		async committed() : Promise<OffsetAndMetadata> {
			return Promise.resolve({...committedOffset})
		},
		
		async isEmpty() {
			return Promise.resolve(highOffset().subtract(lowOffset()).lte(0))
		},
		
		/* istanbul ignore next */
		log(tags, payload) {},

		async seek(soughtOffset : string | Long | LogicalOffset | LogicalLiteralOffset) {
			// resolve the requested offset to a message that has been injected
			const absoluteOffset : Long = isEarliest(soughtOffset) ? lowOffset() :
				isLatest(soughtOffset) ? highOffset() :
				Long.fromValue(soughtOffset)
			const closestIndex = injectedMessages.findIndex(({ offset }) => offset.gte(absoluteOffset))
			const soughtIndex = closestIndex > -1 ? closestIndex : 
				injectedMessages.length - 1 // default to high water
			const nextMessage = injectedMessages[soughtIndex]

			// update the offset we're currently consuming or reset to start or end
			seekToOffset = Long.fromValue(nextMessage.offset).toNumber()

			// replay any messages if necessary
			if (consumedOffset >= seekToOffset) {
				setTimeout(() => { // out of context, to make sure seek completes before we process all th new messages
					injectedMessages.slice(soughtIndex).forEach(writeMessageToStream)
				})
			}
		},

		async send(messages) {
			if (!Array.isArray(messages)) messages = [messages]
			
			return messages.map(createMessage).map((message) => {
				producedMessages.push(message)

				if (message.topic === assignment.topic && message.partition === assignment.partition) {
					injectMessage(message)
				}

				return {
					topicName: message.topic,
					partition: message.partition,
					errorCode: 0,
					offset: message.offset.toString(),
					timestamp: message.timestamp
				}
			})
		},

		async watermarks() : Promise<Watermarks> {
			return {
				highOffset: highOffset().toString(),
				lowOffset: lowOffset().toString()
			}
		},

		topic: assignment.topic,
		partition: assignment.partition,
		// TODO: decide if we want to support prefixes
		// prefix: assignment.prefix
		group: assignment.group
	}

	const initialMessages = initialState.messages.map(createMessage).map(injectMessage)

	const controlledStream : Highland.Stream<Message> = stream.map((messageOrError) => {
		if (messageOrError instanceof Error) {
			throw (messageOrError as Error)
		}

		return messageOrError as InternalMessage
	}).filter(({ offset }) => {
		if (seekToOffset > -1) {
			return Long.fromValue(offset).equals(seekToOffset)
		} else {
			return true
		}
	}).map((message: InternalMessage) => {
		consumedOffset = Long.fromValue(message.offset).toNumber()
		seekToOffset = -1
		return {
			...message,
			offset: message.offset.toString(),
			highWaterOffset: highOffset().toString()
		}
	})

	const [processingPipeline, offsetsStream] = await createPipeline(assignmentContext, processors)
	const processedStream = controlledStream.through(processingPipeline)

	const processingResults = []
	const processing = processedStream.tap((result) => {
		processingResults.push(result)
	}).last().toPromise(Promise)
	const processedOffsets = []
	offsetsStream.each((offset) => {
		processedOffsets.push(offset)
	})

	function inject(payload: InjectMessagePayload) : Message
	function inject(payload: Error): Error
	function inject(payload: any) : any {
		if (payload instanceof Error) {
			injectError(payload)
			return payload
		} else {
			let internal = createMessage(payload)
			injectMessage(internal)
			return {
				...internal,
				offset: internal.offset.toString(),
				highWaterOffset: highOffset().toString()
			}
		}
	}

	return {
		inject,
		committedOffsets,
		async caughtUp() {
			await offsetsStream.observe()
				.map(async (offset) => assignmentContext.caughtUp(`${offset}`))
				.flatMap((awaiting) => H(awaiting))
				.find((isCaughtUp) => isCaughtUp)
				.toPromise(Promise)
		},
		async end() {
			stream.end()
			await processing
		},
		initialMessages,
		processing,
		processingResults,
		processedOffsets,
		producedMessages
	}
}

export default createContext