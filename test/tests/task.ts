import Tap from 'tap'
import createTask from '../../src/task'
import H from 'highland'
import { spy, match } from 'sinon'
import { logLevel as LOG_LEVEL } from 'kafkajs'

import {
	kafkaConfig,
	secureRandom,
	createTopic,
	deleteTopic,
	produceMessages
} from '../helpers'

Tap.setTimeout(60 * 1000)

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

	await t.test('Task.start', async (t) => {
		let testTopic, testGroup, task
		function createTestTask () {
			return createTask({ 
				group: testGroup, 
				connection: { ...kafkaConfig(), logLevel: LOG_LEVEL.ERROR }
				// consumer: {
				// 	sessionTimeout: 3 * 1000,
				// 	heartbeatInterval: 500
				// }
			})
		}


		t.beforeEach(async () => {
			testTopic = `topic-${secureRandom()}`
			testGroup = `group-${secureRandom()}`
			task = createTestTask()
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

			const testSource = task.source(testTopic)
	
			const processingMessages = H()
			const messageProcessor = spy((message) => {
				processingMessages.write(message)
				return message
			})
			const processorSetup = spy((assignment) => {
				t.equal(assignment.topic, testTopic, 'processor setup is called for topic subscribed to')
				t.ok(assignment.partition === 0 || assignment.partition === 1 , 'processor setup is called for partition subscribed to')
				return messageProcessor
			})

			task.processor(testSource, processorSetup)

			await task.start()

			// TODO: replace timeout by consuming log from the start
			await new Promise((r) => setTimeout(r, 1500))
			await produceMessages(testTopic, testMessages)

			const processedMessages = await processingMessages
				.take(testMessages.length).collect().toPromise(Promise)

			t.ok(processorSetup.calledTwice, 'processor setup is called for each received assignment')
			t.ok(task.processingSession, 'exposes a Promise that represents the session processing pipeline output')
			t.ok(task.reassigning, 'exposes a Promise that represents the reassignment process')
		})

		await t.test('requires task to have been created with kafka connection information', async (t) => {
			task = createTask({ group: testGroup })

			await t.rejects(async () => {
				await task.start()
			}, /kafka connection/, 'throws missing connection options error when starting task created without them')
		})

		await t.test('prevents processing of messages when any of the assignment processor setups reject', async (t) => {
			const testSource = task.source(testTopic)

			const processingMessages = H()
			const messageProcessor = spy((message) => {
				processingMessages.write(message)
				return message
			})
			const processorSetup = spy((assignment) => {
				throw new Error('any-processor-setup-error')
				return messageProcessor
			})

			task.processor(testSource, processorSetup)

			const nextError = new Promise((resolve, reject) => task.events.once('error', reject))
			// it should start up fine
			await task.start()

			// but the processing should fail
			await t.rejects(nextError, 'any-processor-setup-error', 'emits error events for any error thrown in assignment processors')
		})

		await t.test('on subsequent rebalances will end processing for active assignments before starting new one', async (t) => {
			const setupTaskInstance = (task) => {
				const testSource = task.source(testTopic)

				const processingMessages = H()
				const messageProcessor = spy((message) => {
					processingMessages.write(message)
					return message
				})
				const processorSetup = spy((assignment) => {
					return messageProcessor
				})
				
				task.processor(testSource, processorSetup)

				const nextSessionStart = () => {
					return new Promise((resolve) => task.events.once('session-start', resolve))
				}

				const sessionStopped = spy()
				task.events.on('session-stop', sessionStopped)

				return { 
					task, 
					processingMessages, 
					messageProcessor, 
					processorSetup, 
					nextSessionStart,
					sessionStopped
				}
			}

			const testMessages = Array(100).fill({}).map((obj, n) => ({
				value: `value-${secureRandom()}`,
				key: `value-${secureRandom()}`,
				partition: n % 2
			}))

			const taskOne = setupTaskInstance(task)
			const taskTwo = setupTaskInstance(createTestTask())

			const taskOneSessionStart = taskOne.nextSessionStart()
			await taskOne.task.start()

			await taskOneSessionStart

			const secondSessionStarts = Promise.all([
				taskOne.nextSessionStart(),
				taskTwo.nextSessionStart()
			])

			await taskTwo.task.start()
			await secondSessionStarts
			
			t.ok(taskOne.processorSetup.calledThrice, 'processor setup is called for every reassigment')
			t.ok(taskOne.processorSetup.thirdCall.calledAfter(taskOne.sessionStopped.firstCall), 'stops running assignments before starting new ones')

			await taskTwo.task.stop()
		})
	})
})