import createContext, { AssignmentContext } from '.'

export default function(assignments, task) {
	return assignments.map(({ topic, partition, group }) => {
		return { topic, partition, group }
	})
}