import H from 'highland'

export interface AssignmentTestInterface {
	inject(payload: { topic: string, partition: number, value: any })
	committedOffsets: string[],
	producedMessages: any[]
}

const createContext = async function({
	assignment,
	processors
}) : Promise<AssignmentTestInterface> {
	var consumedOffset = 0

	const stream = H()
	const injectedMessages = []
	const producedMessages = []

	const injectMessage = (payload : { topic: string, partition: number, value: any }) => {
		const message = {
			...payload, 
			offset: 0
		}

		injectedMessages.push(message)
		stream.write(message)

		return message
	} 

	const context = {
		/* istanbul ignore next */
		async caughtUp(offset) {},
		
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

		async send(messages: Array<{ topic: string, partition: number, value: any}>) {
			messages.forEach((message) => {
				producedMessages.push(message)

				if (message.topic === assignment.topic && message.partition === assignment.partition) {
					injectMessage(message)
				}
			})
		},

		/* istanbul ignore next */
		async watermarks() {},

		stream,
		topic: assignment.topic,
		partition: assignment.partition,
		// TODO: decide if we want to support prefixes
		// prefix: assignment.prefix
		group: assignment.group
	}

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