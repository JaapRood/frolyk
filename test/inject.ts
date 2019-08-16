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