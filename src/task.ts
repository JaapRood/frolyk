import EventEmitter from 'events'
import Source from './source'
import createLocalAssignmentContext from './assignment-contexts/local'

class Task {
	events: EventEmitter
	sources: Array<Source>

	constructor() {
		this.events = new EventEmitter()
		this.sources = []
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

		return {
			...source,
			processors: [...source.processors, setupProcessing]
		}
	}

	inject(assignments: Array<{ topic, partition }>) {
		const contexts = assignments.map((assignment) => {
			const source = this.sources.find(({ topicName }) => topicName === assignment.topic)

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

export default function createTask() : Task {
	return new Task()
}