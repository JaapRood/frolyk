import Source from './source'

interface Assignment {
	topic: string,
	partition: number,
	group: string
}

export interface AssignmentContext {
	topic: string,
	partition: number,
	group: string
}

export default interface {
	({ 
		assignment: Assignment,
		processors: any
	}): AssignmentContext
}