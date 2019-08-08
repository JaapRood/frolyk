import Tap from 'tap'
import createTask from '../src/task'

Tap.test('Task', async (t) => {
	t.test('can be constructed', async (t) => {
		t.doesNotThrow(() => {
			createTask({ group: 'test-group' })
		})
	})

	t.test('Task.source', async (t) => {
		const task = createTask({ group: 'test-group' })


		// TODO: figure out how to test whether something throws in Javascript.. separate tests?
		// Overload the method and throw from that? Learning TypeScript, yay!
		// 
		// t.throws(() => {
		// 	task.source()
		// })

		const source = task.source('test-topic')
		const sameSource = task.source('test-topic')
	})

	t.test('Task.process', async (t) => {
		const task = createTask({ group: 'test-group' })

		const source = task.source('test-topic')

		const testProcessor = () => {}
		const updatedSource = task.processor(source, testProcessor)

		t.equal(updatedSource.processors.length, 1)
		t.equal(updatedSource.processors[0], testProcessor)
		t.ok(task.sources.includes(updatedSource))

		const otherTask = createTask({ group: 'test-group' })

		t.throws(() => {
			otherTask.processor(source, () => {})
		})
	})

	t.test('Task.inject', async (t) => {
		const task = createTask({ group: 'test-group' })

		const source = task.source('test-topic')
		task.processor(source, () => {})

		const testInterface = task.inject([{ topic: 'test-topic', partition: 0 }])
		const otherInterface = task.inject([{ topic: 'not-processing-this-topic', partition: 0 }])
	})
})