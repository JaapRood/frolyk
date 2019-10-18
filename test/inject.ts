import Tap from 'tap'
import createLocalAssignmentContext, { AssignmentTestInterface } from '../src/assignment-contexts/local'
import { spy } from 'sinon'


Tap.test('Injected AssignmentContext', async (t) => {
	let testAssignment = {
		topic: 'test-topic',
		partition: 0,
		group: 'test-group'
	}
	const testProcessor = (setupProcessor, assignment = testAssignment, initialState = {}) => {
		return createLocalAssignmentContext({
			assignment,
			processors: [ setupProcessor ],
			initialState
		})
	}

	await t.test('testInterface.inject', async (t) => {
		let processMessage, testInterface
		t.beforeEach(async () => {
			processMessage = spy()
			testInterface = await testProcessor(async (assignment) => {
				return processMessage
			})
		})

		await t.test('can inject a message to the processor with matching topic and partition', async () => {
			const testMessage = {
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				key: 3,
				value: 'a-test-value'
			}
			const injectedMessage = testInterface.inject(testMessage)

			t.equal(injectedMessage.topic, testMessage.topic)
			t.equal(injectedMessage.partition, testMessage.partition)
			t.type(injectedMessage.key, Buffer, 'injected message key is converted to a Buffer')
			t.type(injectedMessage.value, Buffer, 'injected message value is converted to a Buffer')
			t.deepEqual(JSON.parse(injectedMessage.key.toString()), testMessage.key)
			t.deepEqual(JSON.parse(injectedMessage.value.toString()), testMessage.value)

			t.ok(processMessage.calledWith(injectedMessage))
		})

		await t.test('can inject a message without a key or value', async () => {
			const testMessage = {
				topic: testAssignment.topic,
				partition: testAssignment.partition
			}
			const injectedMessage = testInterface.inject(testMessage)

			t.equal(injectedMessage.key, null)
			t.equal(injectedMessage.value, null)
		})

		await t.test('can inject a message with a Buffer key or value', async () => {
			const testMessage = {
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				key: Buffer.from(JSON.stringify(4)),
				value: Buffer.from('a-test-value')
			}
			const injectedMessage = testInterface.inject(testMessage)

			t.equal(injectedMessage.key, testMessage.key)
			t.equal(injectedMessage.value, testMessage.value)
		})

		await t.test('increments the assigned offsets for each subsequently injected message', async () => {
			const testMessages = Array(3).fill({ topic: testAssignment.topic, partition: testAssignment.partition})
			const injectedMessages = testMessages.map((message) => testInterface.inject(message))

			const producedOffsets = injectedMessages.map((message) => message.offset)
			const expectedOffsets = testMessages.map((message, n) => `${n}`)

			t.deepEqual(producedOffsets, expectedOffsets)
		})

		await t.test('can inject message with a predefined offset', async () => {
			const testMessage = {
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				key: Buffer.from(JSON.stringify(4)),
				value: Buffer.from('a-test-value'),
				offset: '3'
			}
			
			let injectedMessage = testInterface.inject(testMessage)

			t.equal(injectedMessage.key, testMessage.key)
			t.equal(injectedMessage.value, testMessage.value)
			t.equal(injectedMessage.offset, testMessage.offset)

			injectedMessage = testInterface.inject({ ...testMessage, offset: '6' })

			t.equal(injectedMessage.offset, '6', 'allows predefined offsets to be non-contiguous')

			t.throws(() => {
				testInterface.inject( testInterface.inject({ ...testMessage, offset: '6' }))
			}, 'does not allow a predefined offset that isnt higher than the last produced offset')


		})
	})


	await t.test('assignment.watermarks', async (t) => {
		const processMessage = spy()
		const testMessages = [
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				value: 'a-test-value-a'
			},
			{
				topic: 'some-other-topic',
				partition: testAssignment.partition,
				value: 'a-test-value-b'
			},
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition + 1,
				value: 'a-test-value-c'
			}
		]

		let testInterface = await testProcessor(async (assignment) => {
			const watermarks = await assignment.watermarks()

			t.equal(watermarks.highOffset, '3', 'returns offset of the last message in the log + 1 as the high offset')
			t.equal(watermarks.lowOffset, '0', 'returns offset of the first message in the log as the low offset')

			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		testInterface = await testProcessor(async (assignment) => {
			const watermarks = await assignment.watermarks()

			t.equal(watermarks.highOffset, '0', 'returns high offset of 0 when message log is empty')
			t.equal(watermarks.lowOffset, '0', 'returns low offset of 0 when message log is empty')

			return processMessage
		})

		testInterface = await testProcessor(async (assignment) => {
			const watermarks = await assignment.watermarks()

			t.equal(watermarks.highOffset, '6', 'returns high offset of the last message in the log + 1')
			t.equal(watermarks.lowOffset, '3', 'returns low ofset of the first message in the log')

			return processMessage
		}, testAssignment, {
			lowOffset: 3,
			messages: testMessages
		})
	})

	await t.test('assignment.caughtUp', async (t) => {
		const processMessage = spy()
		const testMessages = [
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				value: 'a-test-value-a'
			},
			{
				topic: 'some-other-topic',
				partition: testAssignment.partition,
				value: 'a-test-value-b'
			},
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition + 1,
				value: 'a-test-value-c'
			}
		]

		let testInterface = await testProcessor(async (assignment) => {
			t.equal(await assignment.caughtUp(2), true)
			t.equal(await assignment.caughtUp(1), false)
			t.equal(await assignment.caughtUp(0), false)

			return processMessage
		}, testAssignment, {
			lowOffset: 0,
			messages: testMessages
		})

		// TODO: test use of logical offsets
	})

	await t.test('assignment.send', async (t) => {
		const processMessage = spy()
		const testMessages = [
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				value: 'a-test-value-a'
			},
			{
				topic: 'some-other-topic',
				partition: testAssignment.partition,
				value: 'a-test-value-b'
			},
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition + 1,
				value: 'a-test-value-c'
			}
		]

		const testInterface = await testProcessor(async (assignment) => {
			await assignment.send(testMessages)

			return processMessage
		})

		const producedMessages = testInterface.producedMessages

		t.ok(processMessage.calledOnce, 'injects any messages sent to test assignment back into processor')
		t.deepEqual(testMessages, producedMessages, 'messages can be sent in assignment setup')
	})

	await t.test('assignment.seek', async (t) => {
		const processMessage = spy((message) => message.offset)
		
		const testMessages = [
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				value: 'a-test-value-a',
				offset: '0'
			},
			{
				topic: 'some-other-topic',
				partition: testAssignment.partition,
				value: 'a-test-value-b',
				offset: '1',
			},
			{
				topic: testAssignment.topic,
				partition: testAssignment.partition,
				value: 'a-test-value-c',
				offset: '2'
			}
		]

		let testInterface = await testProcessor(async (assignment) => {
			await assignment.seek(testMessages[1].offset)

			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['1', '2'], 'allows consuming to be fast forwarded to an absolute offset')

		testInterface = await testProcessor(async (assignment) => {
			let processedMessages = 0

			return async (message) => {
				processedMessages++
				if (processedMessages === testMessages.length - 1) {
					await assignment.seek(testMessages[0].offset)
				}
				return message.offset
			} 
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['0', '1', '0', '1', '2'], 'allows consuming to be reversed to an absolute offset')

		testInterface = await testProcessor(async (assignment) => {
			await assignment.seek('3')

			return processMessage
		}, testAssignment, {
			messages: [...testMessages, {
				...testMessages[2],
				offset: '4'
			}]
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['4'], 'will seek to next available offset when seeking to an offset that no longer exists (gc)')

		testInterface = await testProcessor(async (assignment) => {
			await assignment.seek('3')

			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['2'], 'will seek to the high water mark when offset is out of range')

		testInterface = await testProcessor(async (assignment) => {
			let processedMessages = 0

			return async (message) => {
				processedMessages++
				if (processedMessages === testMessages.length - 1) {
					await assignment.seek('earliest')
				}
				return message.offset
			} 
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['0', '1', '0', '1', '2'], 'allows logical seeking to the earliest offset')

		testInterface = await testProcessor(async (assignment) => {
			let processedMessages = 0

			return async (message) => {
				processedMessages++
				if (processedMessages === 1) {
					await assignment.seek('latest')
				}
				return message.offset
			} 
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.processingResults, ['0', '2'], 'allows logical seeking to the latest offset')
	})
})