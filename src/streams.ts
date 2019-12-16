import { Consumer, IHeaders } from 'kafkajs/types'
import H from 'highland'
import { Transform, TransformOptions } from 'stream'

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

class TopicPartitionStream extends Transform {
    topic: string
    partition: number

    constructor({ topic, partition } : { topic: string, partition: number }, streamOptions : TransformOptions = {}) {
        super({
            ...streamOptions,
            objectMode: true,
            highWaterMark: 8
        })
    
        this.topic = topic
        this.partition = partition
    }

    _transform(message : Message, encoding, callback) {
        callback(null, message)
    }
}

class TaskStreams {
    consumer: Consumer
    streams: TopicPartitionStream[]

    constructor(consumer : Consumer) {
        this.consumer = consumer
        this.streams = []
    }

    stream({ topic, partition }: { topic: string, partition: number }) : TopicPartitionStream {
        const stream = this.streams
            .find((topparStream) => topparStream.topic === topic && topparStream.partition === partition)    

        if (!stream) {
            let newStream : TopicPartitionStream = new TopicPartitionStream({ topic, partition})
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

                // whether we drained or stopped consuming this partition, we'll
                // want it to be okay for consumer to fetch for this partition again.
                consumer.resume([{ topic, partitions: [partition] }])
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
                    if (!isRunning() || isStale() || stream.writableEnded) break

                    const more = stream.write({
                        topic: batch.topic,
                        partition: batch.partition,
                        highWaterOffset: batch.highWatermark,
                        offset: message.offset,

                        ...message,

                        timestamp: message.timestamp
                    })

                    checkpoint(message.offset)
                    
                    if (!more && isRunning() && !isStale() && !stream.writableEnded) {
                        pauseConsumption({ topic: batch.topic, partition: batch.partition })
                        break
                    }
                    
                    await heartbeat()
                }

                await heartbeat()
            }
        })
    }
}

export default function create(consumer): TaskStreams {
    return new TaskStreams(consumer)
}