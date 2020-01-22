import { Consumer, IHeaders } from 'kafkajs/types'
import H from 'highland'
import { Transform, TransformOptions } from 'stream'
import { EventEmitter } from 'events'
import Long from 'long'
import Invariant from 'invariant'

export interface Message {
    topic: string
    partition: number
    
    key?: Buffer | string | null
    value: Buffer | string | null

    headers?: IHeaders,
    highWaterOffset: string
    offset: string
    timestamp?: string
}

export interface TopicPartitionStream extends Transform {
    topic: string
    partition: number

    seek(offset: string | Long) : void
}

class SeekOp {
    id: number
    offset: string

    constructor({ id, offset }) {
        this.id = id
        this.offset = offset
    }
}

class TPStream extends Transform implements TopicPartitionStream {
    topic: string
    partition: number
    consumer: Consumer

    private seekOpIdSeq: number
    private observedOpId: number
    
    constructor({ topic, partition, consumer } : { topic: string, partition: number, consumer: Consumer }, streamOptions : TransformOptions = {}) {
        super({
            ...streamOptions,
            objectMode: true,
            readableHighWaterMark: 0,
            writableHighWaterMark: 16,
            emitClose: true,
            autoDestroy: true
        })
    
        this.topic = topic
        this.partition = partition
        this.consumer = consumer

        this.seekOpIdSeq = -1
        this.observedOpId = -1
    }

    _transform(messageOrOp : Message | SeekOp, encoding, callback) {
        if (this.seekOpIdSeq !== this.observedOpId) {
            // there's a pending seek operation to be completed
            if (messageOrOp instanceof SeekOp) {
                let op = messageOrOp
                this.observedOpId = op.id
            }

            return callback()
        } else {
            let message = messageOrOp
            
            callback(null, message)
        }
    }

    seek(offset: string | Long) {
        try {
            offset = Long.fromValue(offset)
        } catch (parseError) {
            Invariant(!parseError, 'Valid offset (parseable as Long) is required to seek stream to offset')
        }

        const { topic, partition } = this

        const operation = new SeekOp({
            id: ++this.seekOpIdSeq,
            offset: offset.toString()
        })

        this.consumer.seek({
            topic,
            partition,
            offset: offset.toString()
        })
        this.write(operation)
    }
}

class TaskStreams {
    private consumer: Consumer
    private streams: TopicPartitionStream[]
    private consumerEvents: EventEmitter

    constructor(consumer : Consumer) {
        this.consumer = consumer
        this.streams = []

        this.consumerEvents = new EventEmitter()
        consumer.on(consumer.events.STOP, (...args) => {
            this.consumerEvents.emit(consumer.events.STOP, ...args)
        })
    }

    stream({ topic, partition }: { topic: string, partition: number }) : TopicPartitionStream {
        const { consumer, consumerEvents } = this
        const stream = this.streams
            .find((topparStream) => topparStream.topic === topic && topparStream.partition === partition)    

        if (!stream) {
            let newStream : TopicPartitionStream = new TPStream({ topic, partition, consumer })
            
            let onConsumerStop = () => newStream.end()
            let onStreamClose = () => {
                this.streams = this.streams.filter((stream) => stream !== newStream)
            }
            let onStreamFinish = () => cleanup()
            let cleanup = () => {
                consumerEvents.removeListener(consumer.events.STOP, onConsumerStop)
                newStream.destroy() // no more writing to this stream
            }

            newStream.once('close', onStreamClose)
            newStream.once('finish', onStreamFinish)
            consumerEvents.once(consumer.events.STOP, onConsumerStop)
            
            this.streams.push(newStream)

            return newStream
        } else {
            return stream
        }
    }

    async start() {
        const { consumer } = this

        const pauseConsumption = async ({ topic, partition } : { topic: string, partition: number }) => {
            const stream = this.stream({ topic, partition })
            // let isDrained = Symbol('drained')


            let onDrain
            let draining = new Promise((resolve, reject) => {
                onDrain = () => resolve()
                stream.once('drain', onDrain)
            })

            let onEnd
            let ending = new Promise((resolve, reject) => {
                onEnd = () => resolve()
                stream.once('end', onEnd)
            })

            consumer.pause([{ topic, partitions: [partition] }])

            try {
                await Promise.race([draining, ending])
            } finally {
                stream.removeListener('drain', onDrain)
                stream.removeListener('end', onEnd)
            }
        }
        
        await consumer.run({
            autoCommit: false, // let us be in full control of committing offsets
            eachBatchAutoResolve: false, // do our own local checkpointing
            partitionsConsumedConcurrently: 4,

            eachBatch: async ({ 
                batch, 
                resolveOffset: checkpoint, 
                commitOffsetsIfNecessary: commitOffset, 
                heartbeat, 
                isRunning,
                isStale
            }) => {
                const stream = this.stream({ topic: batch.topic, partition: batch.partition })
                
                for (let message of batch.messages) {
                    if (!isRunning() || stream.writableEnded || isStale()) break

                    const more = stream.write({
                        topic: batch.topic,
                        partition: batch.partition,
                        highWaterOffset: batch.highWatermark,
                        offset: message.offset,

                        ...message,

                        timestamp: message.timestamp
                    })

                    checkpoint(message.offset)
                    
                    if (!more && isRunning()) {
                        await pauseConsumption({ topic: batch.topic, partition: batch.partition })
                        if (!isRunning() || stream.writableEnded) break
                    }
                    
                    await heartbeat()
                }

                await heartbeat()

                // whether we drained or stopped consuming this partition, we'll
                // want it to be okay for consumer to fetch for this partition again.
                if (isRunning()) {
                    consumer.resume([{ topic: batch.topic, partitions: [batch.partition] }])
                }
            }
        })
    }
}

export default function create(consumer): TaskStreams {
    return new TaskStreams(consumer)
}