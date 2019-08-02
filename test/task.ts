import Tap from 'tap'
import createTask from '../src/task'

Tap.test('Task', async (t) => {
	t.test('can be constructed', async (t) => {
		t.doesNotThrow(() => {
			createTask()
		})
	})

	t.test('Task.source', async (t) => {
		const task = createTask()


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
		const task = createTask()

		const source = task.source('test-topic')

		task.process(source, () => {})

		const otherTask = createTask()

		t.throws(() => {
			otherTask.process(source, () => {})
		})
	})
})