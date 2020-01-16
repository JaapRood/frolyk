import { LogicalOffset, LogicalLiteralOffset } from './offsets'

export default interface Source {
	topicName: string,
	processors: any[],
	offsetReset: LogicalOffset | LogicalLiteralOffset
}
