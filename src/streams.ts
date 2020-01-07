import { Consumer, IHeaders } from 'kafkajs/types'
import H from 'highland'
import { Transform, TransformOptions } from 'stream'
import { EventEmitter } from 'events'

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
}

class TPStream extends Transform implements TopicPartitionStream {
    topic: string
    partition: number
    
    constructor({ topic, partition } : { topic: string, partition: number }, streamOptions : TransformOptions = {}) {
        super({
            ...streamOptions,
            objectMode: true,
            highWaterMark: 8,
            emitClose: true,
            autoDestroy: true
        })
    
        this.topic = topic
        this.partition = partition
    }

    _transform(message : Message, encoding, callback) {
        callback(null, message)
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
            let newStream : TopicPartitionStream = new TPStream({ topic, partition})
            
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
                    if (!isRunning() || stream.writableEnded) break

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