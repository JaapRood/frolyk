import Source from './source'

export interface AssignmentContext {
	topic: string,
	partition: 0,
	group: string
}

export default interface {
	(assignments, task): Array<AssignmentContext>
}