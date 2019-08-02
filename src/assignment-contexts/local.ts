import createContextInterface, { AssignmentContext } from '.'

const createContext : createContextInterface = function({
	assignment,
	processors
}) {
	const context = { 
		topic: assignment.topic,
		partition: assignment.partition,
		group: assignment.group
	}

	return context
}

export default createContext