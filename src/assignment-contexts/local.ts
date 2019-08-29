import H from 'highland'
import Long from 'long'

export interface AssignmentTestInterface {
	inject(payload: { topic: string, partition: number, value: any })
	committedOffsets: string[],
	producedMessages: any[]
}

interface InternalMessage {
	topic: string,
	partition: number,
	value: Buffer | null,
	key: Buffer | null,
	offset: Long
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

	let producedOffset = initialState.lowOffset
	const stream : Highland.Stream<Message> = H()
	const injectedMessages : InternalMessage[] = []
	const producedMessages = []

	const injectMessage = (payload : { 
		topic: string, 
		partition: number, 
		value?: any, 
		key?: any 
	}) : Message => {
		const value = !payload.value ? null :
			Buffer.isBuffer(payload.value) ? payload.value :
			Buffer.from(JSON.stringify(payload.value))

		const key = !payload.key ? null :
			Buffer.isBuffer(payload.key) ? payload.key :
			Buffer.from(JSON.stringify(payload.key))


		const internalMessage = {
			...payload,
			value,
			key,
			offset: Long.fromNumber(++producedOffset - 1)
		}

		const message = {
			...internalMessage,
			offset: internalMessage.offset.toString()
		}

		injectedMessages.push(internalMessage)
		stream.write(message)

		return message
	}

	const highOffset = () : Long => {
		const lastMessage = injectedMessages[injectedMessages.length - 1]
		return lastMessage ? lastMessage.offset.add(1) : Long.fromNumber(0)
	}

	const lowOffset = () : Long => {
		const firstMessage = injectedMessages[0]
		return firstMessage ? firstMessage.offset : Long.fromNumber(0)
	}

	const context = {
		async caughtUp(offset) {
			// TODO: deal with logical offsets
			return Long.fromValue(offset).add(1) >= highOffset()
		},
		
		/* istanbul ignore next */
		async commitOffset(offset, metadata = null) {},
		
		/* istanbul ignore next */
		async isEmpty() {},
		
		/* istanbul ignore next */
		async log(tags, payload) {},
		
		/* istanbul ignore next */
		async pause() {},

		/* istanbul ignore next */
		async resume() {},

		/* istanbul ignore next */
		async heartbeat() {},

		/* istanbul ignore next */
		async seek(offset) {},

		async send(messages: Array<{ topic: string, partition: number, value: any}>) : Promise<void> {
			messages.forEach((message) => {
				producedMessages.push(message)

				if (message.topic === assignment.topic && message.partition === assignment.partition) {
					injectMessage(message)
				}
			})
		},

		async watermarks() : Promise<{ highOffset: string, lowOffset: string }> {
			return {
				highOffset: highOffset().toString(),
				lowOffset: lowOffset().toString()
			}
		},

		stream,
		topic: assignment.topic,
		partition: assignment.partition,
		// TODO: decide if we want to support prefixes
		// prefix: assignment.prefix
		group: assignment.group
	}

	initialState.messages.forEach(injectMessage)

	const processedStream = await processors.reduce(async (s, setupProcessor) => {
		const stream = await s

		const processMessage = await setupProcessor(context)


		return stream
			.map(async (message) => await processMessage(message))
			.flatMap((awaitingProcessing) => H(awaitingProcessing))
	}, Promise.resolve(stream))

	const processedMessages = []
	processedStream.each((message) => {
		processedMessages.push(message)
	})

	return {
		inject: injectMessage,
		committedOffsets: [],
		producedMessages
	}
}

export default createContext