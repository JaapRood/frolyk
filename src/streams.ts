import { Consumer, IHeaders } from 'kafkajs/types'
import H from 'highland'

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

class TaskStreams {
    consumer: Consumer
    streams: Array<{
        topic: string,
        partition: number,
        stream: Highland.Stream<Message>
    }>

    constructor(consumer : Consumer) {
        this.consumer = consumer
        this.streams = []
    }

    stream({ topic, partition }: { topic: string, partition: number }) : Highland.Stream<Message> {
        const stream = this.streams
            .filter((topparStream) => topparStream.topic === topic && topparStream.partition === partition)    
            .map(({ stream }) => stream)
            .find(() => true) // pick the first matching stream

        if (!stream) {
            let newStream : Highland.Stream<Message> = H()
            this.streams.push({
                topic,
                partition,
                stream: newStream
            })

            return newStream
        } else {
            return stream
        }
    }

    async start() {
        const { consumer } = this
        
        await consumer.run({
            autoCommit: false, // let us be in full control of committing offsets
            eachBatchAutoResolve: false, // do our own local checkpointing
            partitionsConsumedConcurrently: 4,

            eachBatch: async ({ 
                batch, 
                resolveOffset: checkpoint, 
                commitOffsetsIfNecessary: commitOffset, 
                heartbeat, 
                isRunning 
            }) => {
                const stream = this.stream({ topic: batch.topic, partition: batch.partition })

                const observingStream = stream.fork()

                const streaming = observingStream.last().toPromise(Promise)

                for (let message of batch.messages) {
                    if (!isRunning() || stream.ended) break

                    let onDrain
                    let draining = new Promise((resolve, reject) => {
                        onDrain = () => resolve()
                        stream.once('drain', onDrain)
                    })

                    const more = stream.write({
                        topic: batch.topic,
                        partition: batch.partition,
                        highWaterOffset: batch.highWatermark,
                        offset: message.offset,

                        ...message,

                        timestamp: message.timestamp
                    })

                    checkpoint(message.offset)
                    await heartbeat()

                    if (!more && isRunning() && !stream.ended) {
                        await Promise.race([
                            draining,
                            streaming
                        ])
                    }

                    stream.removeListener('drain', onDrain)
                }

                await heartbeat()
                observingStream.destroy()
            }
        })
    }
}

export default function create(consumer): TaskStreams {
    return new TaskStreams(consumer)
}