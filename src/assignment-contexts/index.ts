import Source from '../source'

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

export interface OffsetAndMetadata {
	offset: string,
	metadata: string | null
}