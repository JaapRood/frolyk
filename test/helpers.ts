import Crypto from 'crypto'
import { Kafka, logLevel } from 'kafkajs'
import Uuid from 'uuid/v4'
import Config from './config'

export function secureRandom (length = 10) {
    return `${Crypto.randomBytes(length).toString('hex')}-${process.pid}-${Uuid()}`
}

export function createConsumer (options: { logLevel?: logLevel } = {
    logLevel: logLevel.NOTHING
}) {
    const { logLevel, ...consumerOptions } = options
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers, logLevel })


    return kafka.consumer({
        groupId: `group-${secureRandom()}`,
        ...consumerOptions,
        maxWaitTimeInMs: 100
    })
}

export async function createTopic ({ topic, partitions = 1, config = [] }) {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const admin = kafka.admin()

    try {
        await admin.connect()
        await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic, numPartitions: partitions, configEntries: config }],
        })
    } finally {
        admin && (await admin.disconnect())
    }
}

export async function deleteTopic (topic) {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const admin = kafka.admin()

    try {
        await admin.connect()
        await admin.deleteTopics({
            topics: [topic]
        })
    } finally {
        admin && (await admin.disconnect())
    }
}

export async function produceMessages (topic, messages) {
    const kafka = new Kafka({ clientId: 'frolyk-tests', brokers: Config.kafka.brokers })
    const producer = kafka.producer()

    try {
        await producer.connect()
        await producer.send({ acks: 1, topic, messages })
    } finally {
        producer && (await producer.disconnect())
    }
}