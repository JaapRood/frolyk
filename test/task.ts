import Tap from 'tap'
import createTask from '../src/task'
import { spy } from 'sinon'

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
		let task, source

		const testGroup = 'test-group'
		const testTopic = 'test-topic'

		t.beforeEach(async () => {
			task = createTask({ group: testGroup })
			source = task.source(testTopic)
		})

		await t.test('returns a test interface', async (t) => {
			task.processor(source, () => {})

			const testInterfaces = await task.inject([{ topic: 'test-topic', partition: 0 }])

			t.ok(Array.isArray(testInterfaces), 'returns an array of test assignment interfaces')

			const singleInterface = await task.inject({ topic: 'test-topic', partition: 0 })
			t.notOk(Array.isArray(singleInterface), 'returns a single test assignment interface when injecting a single assignment')

			const otherInterface = await task.inject({ topic: 'not-processing-this-topic', partition: 0 })
			t.ok(otherInterface, 'returns a test assignment interface for unknown topics')
		})

		await t.test('test interface can inject messages', async () => {
			const messageProcessor = spy()
			const processorSetup = spy(() => {
				return messageProcessor
			})

			task.processor(source, processorSetup)

			const testInterface = await task.inject({ topic: testTopic, partition: 0 })
			t.ok(processorSetup.calledOnce)

			testInterface.inject({
				topic: testTopic,
				partition: 0,
				value: 'a-test-value'
			})

			t.ok(messageProcessor.calledOnce)
		})	
	})
})