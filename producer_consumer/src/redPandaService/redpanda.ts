import { Kafka, Producer, Consumer, Admin, Partitioners } from 'kafkajs';
import { Config } from '../config';

export class RedpandaService {
    private _kafka: Kafka;
    private _producer!: Producer;
    private _consumer!: Consumer;
    private _admin!: Admin;

    constructor() {
        this._kafka = new Kafka({
            clientId: 'street-service',
            brokers: [`${process.env.REDPANDA_HOST || 'localhost'}:9092`],
            // Add a slight retry logic for stability during 1M item tests
            retry: {
                initialRetryTime: 100,
                retries: 8
            }
        });
    }

    static async init(): Promise<RedpandaService> {
        const service = new RedpandaService();
        await service._connect();
        return service;
    }

    private async _connect() {
        this._admin = this._kafka.admin();
        this._producer = this._kafka.producer({
            createPartitioner: Partitioners.LegacyPartitioner
        });

        // FIX: Changed groupId to 'v2' to force a reset of the read pointer (offsets)
        this._consumer = this._kafka.consumer({ groupId: 'street-consumers-v2' });

        await this._admin.connect();
        await this._producer.connect();
        await this._consumer.connect();

        const topic = Config.rabbitMq.queueConfig.queue;
        const topics = await this._admin.listTopics();
        if (!topics.includes(topic)) {
            await this._admin.createTopics({
                topics: [{
                    topic,
                    numPartitions: 3,
                    replicationFactor: 1
                }],
            });
            console.log(`[Redpanda] Topic ${topic} created.`);
        }
    }

    async publish(message: any, options?: { topic?: string }) {
        const topic = options?.topic || Config.rabbitMq.queueConfig.queue;
        await this._producer.send({
            topic,
            messages: [
                { value: JSON.stringify(message) }
            ],
        });
    }

    async subscribe(topic: string, callback: (msg: any) => Promise<void>) {
        // 1. Tell Redpanda which topic to watch
        await this._consumer.subscribe({ topic, fromBeginning: true });

        // 2. Start the internal polling loop
        await this._consumer.run({
            autoCommit: true,
            eachMessage: async ({ message, partition, heartbeat }) => {
                if (message.value) {
                    // FIX: Convert Buffer to String so JSON.parse in consumer.ts works
                    await callback({
                        content: message.value.toString(),
                    });
                }
                // Periodic heartbeat tells Redpanda the consumer is still alive
                await heartbeat();
            },
        });
    }

    async disconnect() {
        try {
            await Promise.all([
                this._producer.disconnect(),
                this._consumer.disconnect(),
                this._admin.disconnect()
            ]);
        } catch (e) {
            console.error("Error during Redpanda disconnect", e);
        }
    }
}