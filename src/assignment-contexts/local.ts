import H from 'highland'

const createContext = async function({
	assignment,
	processors
}) {
	var consumedOffset = 0

	const stream = H()

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

		/* istanbul ignore next */
		async send(messages) {},

		/* istanbul ignore next */
		async watermarks() {},

		stream,
		topic: assignment.topic,
		partition: assignment.partition,
		// TODO: decide if we want to support prefixes
		// prefix: assignment.prefix
		group: assignment.group
	}


	return {
		inject(payload : { topic: string, partition: number, value: any }) {
			const message = {
				...payload, 
				offset: 0
			}

			stream.write(message)

			return message
		},
		committedOffsets: [],
		sentMessages: []
	}
}

export default createContext