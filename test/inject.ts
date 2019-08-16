import Tap from 'tap'
import createLocalAssignmentContext, { AssignmentTestInterface } from '../src/assignment-contexts/local'
import { spy } from 'sinon'


Tap.test('Injected AssignmentContext', async (t) => {
	let testAssignment = {
		topic: 'test-topic',
		partition: 0,
		group: 'test-group'
	}
	const testProcessor = (setupProcessor, assignment = testAssignment) => {
		return createLocalAssignmentContext({
			assignment,
			processors: [ setupProcessor ]
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
})