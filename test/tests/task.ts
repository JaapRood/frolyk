import Tap from 'tap'
import createTask from '../../src/task'
import H from 'highland'
import { spy, match } from 'sinon'

import {
	kafkaConfig,
	secureRandom,
	createTopic,
	deleteTopic,
	produceMessages
} from '../helpers'

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

			const testMessage = {
				topic: testTopic,
				partition: 0,
				value: 'a-test-value'
			}
			const injectedMessage = testInterface.inject(testMessage)

			t.ok(messageProcessor.calledOnce)
			t.ok(messageProcessor.calledWith(injectedMessage))
		})
	})

	t.test('Task.start', async (t) => {
		let testTopic, testGroup, task
		t.beforeEach(async () => {
			testTopic = `topic-${secureRandom()}`
			testGroup = `group-${secureRandom()}`
			task = createTask({ group: testGroup, connection: kafkaConfig() })
			await createTopic({ topic: testTopic, partitions: 2 })
		})

		t.afterEach(async () => {
			if (task) await task.stop()
			if (testTopic) await deleteTopic(testTopic)
		})

		await t.test('connects a Consumer, joins group and sets up processing pipeline for each assignment', async (t) => {
			const testMessages = Array(100).fill({}).map(() => ({
				value: `value-${secureRandom()}`,
				key: `value-${secureRandom()}`,
				partition: 0
			}))

			await produceMessages(testTopic, testMessages)

			const testSource = task.source(testTopic)
	
			const processingMessages = H()
			const messageProcessor = spy((message) => processingMessages.write(message))
			const processorSetup = spy((assignment) => {
				t.equal(assignment.topic, testTopic, 'processor setup is called for topic subscribed to')
				t.ok(assignment.partition === 0 || assignment.partition === 1 , 'processor setup is called for partition subscribed to')
				return messageProcessor
			})

			task.processor(testSource, processorSetup)

			await task.start()

			const processedMessages = await processingMessages
				.tap((message) => console.log('processed message', message))
				.take(testMessages.length).collect().toPromise(Promise)

			// await task.stop()

			t.ok(processorSetup.calledTwice, 'processor setup is called for each received assignment')
		})
	})
})