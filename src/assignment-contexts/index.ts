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

export interface NewMessage {
	topic: string,
	value: Buffer | string | null,
	
	partition?: number,
	key?: Buffer | string | null,
	
	timestamp?: string,
	headers?: {
		[key: string]: Buffer | string
	}
}

export interface ProducedMessageMetadata {
	topicName: string,
	partition: number,
	errorCode: number,
	offset: string,
	timestamp: string
}

export interface Watermarks {
	highOffset: string,
	lowOffset: string
}