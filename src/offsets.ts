export enum LogicalOffset {
    Latest = -1,
    Earliest = -2
}

export enum LogicalLiteralOffset {
    // latest
    End = 'end',
    Latest = 'latest',
    Largest = 'largest',


    // earliest
    Beginning = 'beginning',
    Earliest = 'earliest',
    Smallest = 'smallest'
}

const earliestLogicalOffsets: any = [
    LogicalOffset.Earliest,
    LogicalLiteralOffset.Beginning,
    LogicalLiteralOffset.Earliest,
    LogicalLiteralOffset.Smallest
]

const latestLogicalOffsets: any = [
    LogicalOffset.Latest,
    LogicalLiteralOffset.End,
    LogicalLiteralOffset.Latest,
    LogicalLiteralOffset.Largest
]

export function isEarliest(offset : any) {
    return offset === LogicalOffset.Earliest || earliestLogicalOffsets.includes(offset)
}

export function isLatest(offset : any) {
    return offset === LogicalOffset.Latest || latestLogicalOffsets.includes(offset)
}