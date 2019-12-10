import EventEmitter from 'events'
import Source from './source'
import createLocalAssignmentContext, { AssignmentTestInterface } from './assignment-contexts/local'
import createKafkaAssignmentContext from './assignment-contexts/kafka'
import { Kafka, logLevel as LOG_LEVELS } from 'kafkajs'
import H from 'highland'
import _flatMap from 'lodash.flatmap'
import Uuid from 'uuid/v4'

export { AssignmentTestInterface }

class Task {
	events: EventEmitter
	sources: Array<Source>
	group: string
	options: {
		connection?: any,
		consumer?: any
	}

	consumer?: any
	processing?: any

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
		const sessionAssignmentContexts = H(consumer.events.GROUP_JOIN, consumerEvents, ({ payload: { memberAssignment } }) => {
			const topicNames = Object.keys(memberAssignment)
			const topicPartitions = topicNames.map((topic) => {
				return { topic, partitions: memberAssignment[topic] }
			})
			consumer.pause(topicPartitions)
	
			return _flatMap(topicPartitions, ({ topic, partitions }) => {
				return partitions.map((partition) => ({ topic, partition }))
			})
		}).flatMap((assignments : [{ topic: string, partition: number }]) => {
			// TODO: stop any currently running assignmentes before we start setting up new ones
			return H(assignments)
				.map(async ({ topic, partition }) => {
					const source = this.sources.find(({ topicName }) => topicName === topic)
					
					const assignment = { topic, partition, group: this.group }
					const processors = source ? source.processors : []
					// TODO: inject correct assignment stream
					const messagesStream = H([])

					return createKafkaAssignmentContext({ assignment, processors, messagesStream })
				})
				.map((awaiting) => H(awaiting))
				.mergeWithLimit(4) // setup 4 assignments at once
				.collect()
				.errors((err, push) => {
					// this.log(['task', 'assignments', 'setup', 'error'], err, 'Error in setting up assignments processing pipeline')
					push(err) // rethrow, as we want the pipeline to fully fail now
				})
		})

		const processingAssignments = sessionAssignmentContexts
			.map(async (sessionContexts) => {
				// start processing all session contexts
				await Promise.all(sessionContexts.map((context) => context.start()))

				return H(sessionContexts)
					// .map((context) => context.stream)
			})
			.map((awaiting) => H(awaiting)) // only a single session of processing at once
			.merge() // process all active assignments concurrently

		const processingMessages = processingAssignments
			.map((assignmentContext) => assignmentContext.stream)
			.merge() // process all messages concurrently
			.last() // keep track of the last processing result


		await consumer.connect()

		const topicNames = this.sources.map(({ topicName }) => topicName)
		for (let topic of topicNames) {
			// TODO: add handling of offset resets
			await consumer.subscribe({ topic })
		}

		this.processing = processingMessages.toPromise(Promise)
	}
}

export default function createTask(config : { group: string }) : Task {
	return new Task(config)
}