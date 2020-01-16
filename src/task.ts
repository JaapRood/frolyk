import EventEmitter from 'events'
import Source from './source'
import createLocalAssignmentContext, { AssignmentTestInterface } from './assignment-contexts/local'
import createKafkaAssignmentContext from './assignment-contexts/kafka'
import { Kafka, logLevel as LOG_LEVELS } from 'kafkajs'
import createStreams, { Message } from './streams'
import H from 'highland'
import _flatMap from 'lodash.flatmap'
import Uuid from 'uuid/v4'

export { AssignmentTestInterface }

var idSeq = 0

class Task {
	id: number
	events: EventEmitter
	sources: Array<Source>
	group: string
	options: {
		admin?: any,
		connection?: any,
		consumer?: any
	}

	admin?: any
	consumer?: any
	kafka?: any
	private streams?: any
	reassigning: Promise<void>
	assignedContexts: any[]
	processingSession?: Promise<any>
	private sessionSeq: number

	constructor({ group, connection, consumer, admin } : { group: string, connection?: any, consumer?: any, admin?: any }) {
		this.id = idSeq++
		this.events = new EventEmitter()
		this.sources = []
		this.group = group
		this.options = {
			admin,
			connection,
			consumer
		}

		this.assignedContexts = []
		this.sessionSeq = 0
		this.reassigning = Promise.resolve()
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
		const kafka = this.kafka = new Kafka({
			clientId,
			...connectionConfig
		})

		const consumerConfig = this.options.consumer || {}
		const adminConfig = this.options.admin || {}
		
		const consumer = this.consumer = kafka.consumer({
			...consumerConfig,
			groupId: `${this.group}`
		})
		const admin = this.admin = kafka.admin(adminConfig)
		const streams = this.streams = createStreams(consumer)
		const consumerEvents = new EventEmitter()
		consumer.on(consumer.events.GROUP_JOIN, (...args) => consumerEvents.emit(consumer.events.GROUP_JOIN, ...args))


		// TODO: add handling of consumer crashes, fetches, stopping, disconnects, batch stats collection, etc.
	
		const sessionAssignmentContexts = H(consumer.events.GROUP_JOIN, consumerEvents, ({ payload: { memberAssignment } }) => {
			const topicNames = Object.keys(memberAssignment)
			const topicPartitions = topicNames.map((topic) => {
				return { topic, partitions: memberAssignment[topic] }
			})
	
			return _flatMap(topicPartitions, ({ topic, partitions }) => {
				return partitions.map((partition) => ({ topic, partition }))
			})
		}).each((newAssignments) => {
			this.receiveAssignments(newAssignments)
		})

		await consumer.connect()

		const topicNames = this.sources.map(({ topicName }) => topicName)
		for (let topic of topicNames) {
			// TODO: add handling of offset resets
			await consumer.subscribe({ topic })
		}

		streams.start()
	}

	async stop() {
		const { consumer, events } = this

		if (consumer) {
			await consumer.disconnect()
		}
		// TODO: add teardown of processing pipeline
		events.emit('stop')
	}

	private receiveAssignments(newAssignments) {
		this.events.emit('assignment-receive')
		
		this.reassigning = this.reassign(newAssignments).catch((err) => {
			this.events.emit('error', err)
		})
	}

	private async reassign(newAssignments) {
		const reassignmentSeq = ++this.sessionSeq
		await this.reassigning // wait for previous reassigment to have finished first

		// TODO: Figure out how we can test this. The hard bit is figuring out how we can trigger
		// two additional rebalances while we're in the middle of setting a current one up. 
		/* istanbul ignore next */
		if (reassignmentSeq !== this.sessionSeq) {
			// Additional reassignments were called since this one, while we were waiting for the last to finish,
			// so lets discard these outdated assignments.
			return
		}

		const { admin, consumer, kafka, streams } = this
		const currentContexts = this.assignedContexts

		await Promise.all(currentContexts.map(async (context) => {
			const { topic, partition } = context
			const stream = streams.stream({ topic, partition })
			await context.stop()
			stream.end()
		}))

		if (this.processingSession) {
			this.events.emit('session-stop')
		}


		// We're using Highland here to control concurrency, limiting ourselves to setting up 4 assignments
		// concurrently at any given time.
		const newSessionContexts = await H(newAssignments)
			.filter(({ topic, partition }) => !!this.sources.find(({ topicName }) => topicName === topic))
			.map(async ({ topic, partition }) => {
				const source = this.sources.find(({ topicName }) => topicName === topic)

				const assignment = { topic, partition, group: this.group }
				const { processors } = source
				const stream = streams.stream({ topic, partition })

				return createKafkaAssignmentContext({ 
					assignment, 
					admin, 
					consumer, 
					createProducer: (...args) => kafka.producer(...args),
					processors, 
					stream 
				})
			})
			.map((awaiting) => H(awaiting))
			.mergeWithLimit(4) // setup 4 assignments at once
			.collect()
			// TODO: add specific logging for failing of assignment setup
			.toPromise(Promise)


		// wait for all processing of previous session to have ended
		if (this.processingSession) await this.processingSession
		
		this.assignedContexts = newSessionContexts

		// start processing for all assignments concurrently
		await Promise.all(newSessionContexts.map((context) => context.start()))

		this.events.emit('session-start', reassignmentSeq)

		this.processingSession = H(newSessionContexts)
			.map((context) => context.stream)
			.merge() // process all messages within a session at the same time
			.last() // hold on to last processed result
			.toPromise(Promise) // allow monitoring of when processing ends

		this.processingSession.catch((err) => {
			this.events.emit('error', err)
		})
	}

}

export default function createTask(config) : Task {
	return new Task(config)
}