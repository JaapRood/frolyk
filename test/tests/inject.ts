import Tap from 'tap'
import createLocalAssignmentContext, { AssignmentTestInterface } from '../../src/assignment-contexts/local'
import { spy } from 'sinon'
import Long from 'long'

import { secureRandom } from '../helpers'

Tap.test('Injected AssignmentContext', async (t) => {
	let testAssignment = {
		topic: 'test-topic',
		partition: 0,
		group: 'test-group'
	}
	const testProcessor = (setupProcessor, assignment = testAssignment, initialState = {}, sourceOptions={}) => {
		return createLocalAssignmentContext({
			assignment,
			processors: [ setupProcessor ],
			initialState,
			...sourceOptions
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
			
			await testInterface.caughtUp()
			
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

		await t.test('can inject a message without a topic or partition', async () => {
			const testMessage = {}
			const injectedMessage = testInterface.inject(testMessage)

			t.equal(injectedMessage.topic, testAssignment.topic)
			t.equal(injectedMessage.partition, testAssignment.partition)
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

		await t.test('can inject error', async () => {
			const testError = new Error('error-to-test-handling')
			testInterface.inject(testError)

			t.rejects(async () => {
				await testInterface.processing
			}, 'error-to-test-handling', 'injected errors are propagated through the processing pipeline')
		})
	})

	await t.test('testInterface.end will end processing with test interface', async (t) => {
		const testMessages = Array(100).fill({}).map(() => ({
			value: `value-${secureRandom()}`,
			key: `value-${secureRandom()}`
		}))
		
		const testInterface = await testProcessor(async (assignment) => async (message) => {
			await new Promise((r) => setTimeout(r, 10))
			return message.offset
		})

		const injectedMessages = testMessages.map((msg) => testInterface.inject(msg))
		await testInterface.end()

		t.equivalent(
			injectedMessages.map((msg) => msg.offset),
			testInterface.processingResults
		, 'completes once all processing has finished')
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
			const [firstMessage, ...otherMessages] = testMessages
			await assignment.send(firstMessage)
			await assignment.send(otherMessages)

			return processMessage
		})

		const producedMessages = testInterface.producedMessages

		t.ok(processMessage.calledOnce, 'injects any messages sent to test assignment back into processor')
		t.deepEqual(
			testMessages, 
			producedMessages.map(({ topic, partition, value }) => ({ 
				topic, 
				partition, 
				value: JSON.parse(value.toString()) 
			}))
		, 'messages can be sent in assignment setup')
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
			await assignment.seek('5')

			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		testInterface.inject({
			value: 'a-test-value-d'
		})
		await testInterface.end()

		t.deepEqual(testInterface.processingResults, ['3'], 'will seek to the high water mark by default when offset is beyond offset range')

		testInterface = await testProcessor(async (assignment) => {
			await assignment.seek('0')

			return processMessage
		}, testAssignment, {
			lowOffset: '2',
			messages: testMessages.map((msg) => ({
				...msg,
				offset: Long.fromValue(msg.offset).add(2).toString()
			}))
		})

		testInterface.inject({
			value: 'a-test-value-d'
		})
		await testInterface.end()

		t.deepEqual(testInterface.processingResults, [`${testMessages.length + 2}`], 'will seek to the high water mark by default when offset is before offset range')
		
		testInterface = await testProcessor(async (assignment) => {
			await assignment.seek('5')

			return processMessage
		}, testAssignment, {
			messages: testMessages
		}, { offsetReset: 'earliest' })

		await testInterface.end()

		t.deepEqual(testInterface.processingResults, ['0', '1', '2'], 'will seek to the lower water mark when offsetReset set to earliest when offset is beyond offset range')
		
		testInterface = await testProcessor(async (assignment) => {
			await assignment.seek('1')

			return processMessage
		}, testAssignment, {
			lowOffset: '2',
			messages: testMessages.map((msg) => ({
				...msg,
				offset: Long.fromValue(msg.offset).add(2).toString()
			}))
		}, { offsetReset: 'earliest' })

		await testInterface.end()

		t.deepEqual(testInterface.processingResults, ['2', '3', '4'], 'will seek to the lower water mark when offsetReset set to earliest when offset is before offset range')

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
			await assignment.seek('latest')

			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		testInterface.inject({
			value: 'a-test-value-d'
		})

		await testInterface.end()

		t.deepEqual(testInterface.processingResults, ['3'], 'allows logical seeking to the latest offset')
	})

	await t.test('assignment.commitOffset', async (t) => {
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
			await assignment.commitOffset(testMessages[1].offset)

			return processMessage
		}, testAssignment)

		t.deepEqual(testInterface.committedOffsets, [{ offset: '1', metadata: null }], 'allows absolute offsets to be committed during assignment setup')

		
		testInterface = await testProcessor(async (assignment) => {
			return async ({ offset }) => {
				await assignment.commitOffset(offset)

				return offset
			}
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.committedOffsets, [
			{ offset: '0', metadata: null },
			{ offset: '1', metadata: null },
			{ offset: '2', metadata: null }
		], 'allows absoulte offsets to be committed during processing of messages')


		testInterface = await testProcessor(async (assignment) => {
			await assignment.commitOffset(testMessages[1].offset, 'test-metadata')

			return processMessage
		}, testAssignment)

		t.deepEqual(testInterface.committedOffsets, [{ offset: '1', metadata: 'test-metadata' }], 'allows a string of metatdata to be committed with the offset')


		await testProcessor(async (assignment) => {
			t.rejects(() => assignment.commitOffset('not-an-offset'))

			return processMessage
		}, testAssignment)

		await testProcessor(async (assignment) => {
			t.rejects(() => assignment.commitOffset('-2'))

			return processMessage
		}, testAssignment)
	})

	await t.test('assignment.committed', async (t) => {
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

		await testProcessor(async (assignment) => {
			t.deepEqual(
				await assignment.committed(), 
				{ offset: -1, metadata: null },
				'allows currently committed offset to be queried during assignment setup')
			await assignment.commitOffset('2', 'test-metadata')
			t.deepEqual(
				await assignment.committed(),
				{ offset: '2', metadata: 'test-metadata' },
				'returns metadata set with committed offset'
			)

			return processMessage
		}, testAssignment)

		let testInterface = await testProcessor(async (assignment) => {
			return async ({ offset }) => {
				await assignment.commitOffset(offset)

				return await assignment.committed()
			}
		}, testAssignment, {
			messages: testMessages
		})

		await testInterface.caughtUp()

		t.deepEqual(testInterface.committedOffsets, testInterface.processingResults, 'allows committed offset to be queried during processing of messages')
	})

	await t.test('assignment.isEmpty', async (t) => {
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
				partition: testAssignment.partition,
				value: 'a-test-value-c'
			}
		]

		await testProcessor(async (assignment) => {
			t.equal(await assignment.isEmpty(), true, 'resolves to true when log is empty')
			return processMessage
		}, testAssignment)

		await testProcessor(async (assignment) => {
			t.equal(await assignment.isEmpty(), false, 'resolves to false when log contains messages to consume')
			return processMessage
		}, testAssignment, {
			messages: testMessages
		})

		await testProcessor(async (assignment) => {
			t.equal(await assignment.isEmpty(), true, 'resolves to true when log contains no messages to consume, but might have in the past')
			return processMessage
		}, testAssignment, {
			lowOffset: 3
		})
	})
})