import H from 'highland'
import Long from 'long'

import { AssignmentContext, OffsetAndMetadata, NewMessage, ProducedMessageMetadata, Watermarks } from './index'
import { LogicalOffset, LogicalLiteralOffset, isEarliest, isLatest } from '../offsets'

export interface AssignmentTestInterface {
	inject(payload: { topic: string, partition: number, value: any })
	committedOffsets: OffsetAndMetadata[],
	initialMessages: Message[],
	caughtUp() : Promise<void>,
	processingResults: any[],
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

export interface Message {
	topic: string,
	partition: number,
	value: Buffer | null,
	key: Buffer | null,
	offset: string
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
	const stream : Highland.Stream<Message> = H()
	const committedOffsets : OffsetAndMetadata[] = []
	const injectedMessages : InternalMessage[] = []
	const producedMessages = []

	const createMessage = (payload: {
		topic: string,
		partition?: number,
		value?: any,
		key?: any,
		offset?: string,
		timestamp?: string
	}): InternalMessage => {
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
			partition: 0,
			timestamp: `${Date.now()}`,
			...payload,
			value,
			key,
			offset
		}
	}

	const injectMessage = (message : InternalMessage) : Message => {
		injectedMessages.push(message)

		return writeMessageToStream(message)
	}

	const writeMessageToStream = (internalMessage : InternalMessage) : Message => {
		const message = {
			...internalMessage,
			offset: internalMessage.offset.toString()
		}

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

	const context: AssignmentContext= {
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

		seek(soughtOffset : string | Long | LogicalOffset | LogicalLiteralOffset) {
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

	const controlledStream = stream.filter(({ offset }) => {
		if (seekToOffset > -1) {
			return Long.fromValue(offset).equals(seekToOffset)
		} else {
			return true
		}
	}).map((message: Message) => {
		consumedOffset = Long.fromValue(message.offset).toNumber()
		seekToOffset = -1
		return message
	})

	const processedStream = await processors.reduce(async (s, setupProcessor) => {
		const stream = await s

		const processMessage = await setupProcessor(context)

		return stream.map(async (message) => await processMessage(message))
			.flatMap((awaitingProcessing) => H(awaitingProcessing))
	}, Promise.resolve(controlledStream))

	const processingResults = []
	processedStream.each((result) => {
		processingResults.push(result)
	})

	return {
		inject: (payload) => {
			const internal = createMessage(payload)
			return injectMessage(internal)
		},
		committedOffsets,
		async caughtUp() {
			await processedStream.observe()
				.map(async () => context.caughtUp(`${consumedOffset}`))
				.flatMap((awaiting) => H(awaiting))
				.find((isCaughtUp) => isCaughtUp)
				.toPromise(Promise)
		},
		initialMessages,
		processingResults,
		producedMessages
	}
}

export default createContext