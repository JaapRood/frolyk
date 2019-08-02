import EventEmitter from 'events'
import Source from './source'
import createLocalAssignmentContext from './assignment-contexts/local'

class Task {
	events: EventEmitter
	sources: Array<Source>
	group: string

	constructor({ group } : { group: string }) {
		this.events = new EventEmitter()
		this.sources = []
		this.group = group
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

	process(source: Source, setupProcessing) {
		const existingSource = this.sources.find(({ topicName }) => source.topicName === topicName)
		if (!existingSource) {
			throw new Error('Source must be created through same task that processes it')
		}

		existingSource.processors.push(setupProcessing)

		return existingSource
	}

	inject(assignments: Array<{ topic: string, partition: number }>) {
		const group = this.group

		const contexts = assignments.map(({ topic, partition }) => {
			const source = this.sources.find(({ topicName }) => topicName === topic)

			const assignment = { topic, partition, group }
			const processors = source ? source.processors : []

			return createLocalAssignmentContext({ assignment, processors })
		})

		return {
			/* istanbul ignore next */
			inject() {},
			committedOffsets: [],
			sentMessages: []
		}
	}
}

export default function createTask(config : { group: string }) : Task {
	return new Task(config)
}