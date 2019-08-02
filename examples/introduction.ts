import createTask from '../src/task'

const task = createTask()

const locationEvents = task.source('location-events')

task.process(locationEvents, async (assignment) => {
	// Called when Consumer receives assignment through a rebalance, or manual assignment.__dirname

	// Do any setup work here.

	const countsPerTimeWindow = {} // connect to Postgres? Fetch a store from somewhere else for local use?

	return async (message, context) => {
		const location = parseLocation(message.value)

		const win = getWindow(location.timestamp)

		const existingCount = countsPerTimeWindow[win] || 0
		const newCount = existingCount + 1

		countsPerTimeWindow[win] = newCount

		// Process a single message
		context.send('location-counts', newCount)
		context.commit()
	}
})

// task.inject()

await task.start()