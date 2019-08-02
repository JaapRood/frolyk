import EventEmitter from 'events'

interface Source {
	topicName: string,
	processors: []
}

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
}

export default function createTask() : Task {
	return new Task()
}