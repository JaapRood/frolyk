import Source from '../source'
import Long from 'long'
import { LogicalOffset, LogicalLiteralOffset } from '../offsets'

interface Assignment {
	topic: string,
	partition: number,
	group: string
}

export interface AssignmentContext {
	topic: string,
	partition: number,
	group: string,

	caughtUp(offset: string | Long): Promise<boolean>,
	commitOffset(newOffset: string | Long, metadata: string | null): Promise<void>,
	committed(): Promise<OffsetAndMetadata>,
	isEmpty(): Promise<boolean>,
	log(tags, payload): any,
	seek(offset: string | Long | LogicalOffset | LogicalLiteralOffset): Promise<void>,
	send(messages: NewMessage | NewMessage[]): Promise<ProducedMessageMetadata[]>,
	watermarks(): Promise<Watermarks>
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
	offset?: string,
	timestamp?: string,
	baseOffset?: string,
	logAppendTime?: string,
	logStartOffset?: string
}

export interface Watermarks {
	highOffset: string,
	lowOffset: string
}