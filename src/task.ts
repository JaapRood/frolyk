import EventEmitter from 'events'
import Source from './source'
import createLocalAssignmentContext, { AssignmentTestInterface } from './assignment-contexts/local'
import { Kafka, logLevel as LOG_LEVELS } from 'kafkajs'
import Uuid from 'uuid/v4'

export { AssignmentTestInterface }

class Task {
	events: EventEmitter
	sources: Array<Source>
	group: string
	options: {
		consumer?: any,
		connection?: any
	}

	consumer?: any

	constructor({ group, connection, consumer } : { group: string, connection?: any, consumer?: any }) {
		this.events = new EventEmitter()
		this.sources = []
		this.group = group
		this.options = {
			connection,
			consumer
		}
	}

	source(topicName) : Source {
		const existingSource = this.sources.find(({ topicName: t }) => {
			return t === topicName
		})

		if (existingSource) return existingSource

		const newSource : Source =  {
			topicName,
			processors: []
		}

		this.sources.push(newSource)

		return newSource
	}

	processor(source: Source, setupProcessing) {
		const existingSource = this.sources.find(({ topicName }) => source.topicName === topicName)
		if (!existingSource) {
			throw new Error('Source must be created through same task that processes it')
		}

		existingSource.processors.push(setupProcessing)

		return existingSource
	}

	async inject(assignments: { topic: string, partition: number}) : Promise<AssignmentTestInterface>
	async inject(assignments: Array<{ topic: string, partition: number }>) : Promise<Array<AssignmentTestInterface>>
	async inject(assignments: any) {
		const multiple = Array.isArray(assignments)

		assignments = [].concat(assignments) // normalize to array

		const group = this.group

		const contexts = await Promise.all(assignments.map(async ({ topic, partition }) => {
			const source = this.sources.find(({ topicName }) => topicName === topic)

			const assignment = { topic, partition, group }
			const processors = source ? source.processors : []

			return await createLocalAssignmentContext({ assignment, processors })
		}))

		return multiple ? contexts : contexts[0]
	}

	async start() {
		if (!this.options.connection) {
			throw new Error('Task must be configured with kafka connection options to start')
		}

		const connectionConfig = this.options.connection

		const clientId = `frolyk-${Uuid()}`
		const kafka = new Kafka({
			clientId,
			...connectionConfig
		})

		const consumerConfig = this.options.consumer || {}
		
		const consumer = this.consumer = kafka.consumer({
			...consumerConfig,
			groupId: `${this.group}`
		})
		const consumerEvents = new EventEmitter()
		consumer.on(consumer.events.GROUP_JOIN, (...args) => consumerEvents.emit(consumer.events.GROUP_JOIN, ...args))


		// TODO: add handling of consumer crashes, fetches, stopping, disconnects, batch stats collection, etc.

	
		// TODO: add group join handling

		await consumer.connect()

		const topicNames = this.sources.map(({ topicName }) => topicName)
		for (let topic of topicNames) {
			// TODO: add handling of offset resets
			await consumer.subscribe({ topic })
		}
	}
}

export default function createTask(config : { group: string }) : Task {
	return new Task(config)
}