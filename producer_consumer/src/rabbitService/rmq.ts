// rabbitService/rmq.ts
import * as amqplib from 'amqplib';
import { Config } from '../config';

export class RabbitmqService {
    private _connection!: amqplib.Connection;
    private _publishChannel!: amqplib.ConfirmChannel;
    private _consumeChannel!: amqplib.Channel;

    static async init(): Promise<RabbitmqService> {
        const service = new RabbitmqService();
        await service._connect();
        return service;
    }

    private async _connect() {
        this._connection = await amqplib.connect(Config.rabbitMq.connection);

        // Confirm channel for publishing
        this._publishChannel = await this._connection.createConfirmChannel();
        await this._publishChannel.assertExchange(Config.rabbitMq.queueConfig.exchange, 'topic', { durable: true });

        // Dead letter queue
        await this._publishChannel.assertQueue(`${Config.rabbitMq.queueConfig.queue}.dlq`, { durable: true });

        // Normal queue with DLQ config
        await this._publishChannel.assertQueue(Config.rabbitMq.queueConfig.queue, {
            durable: true,
            deadLetterExchange: '', // default exchange
            deadLetterRoutingKey: `${Config.rabbitMq.queueConfig.queue}.dlq`,
            maxLength: 100000 // optional: avoid unbounded queue growth
        });

        await this._publishChannel.bindQueue(
            Config.rabbitMq.queueConfig.queue,
            Config.rabbitMq.queueConfig.exchange,
            '#'
        );

        // Channel for consuming
        this._consumeChannel = await this._connection.createChannel();
        await this._consumeChannel.prefetch(50); // match concurrency
    }

    get consumeChannel(): amqplib.Channel {
        return this._consumeChannel;
    }

    async publish(message: any, options?: { routingKey?: string }) {
        const routingKey = options?.routingKey || '#';
        return new Promise<void>((resolve, reject) => {
            this._publishChannel.publish(
                Config.rabbitMq.queueConfig.exchange,
                routingKey,
                Buffer.from(JSON.stringify(message)),
                { contentType: 'application/json', persistent: true },
                (err, ok) => (err ? reject(err) : resolve())
            );
        });
    }

    async subscribe(queueName: string, callback: (msg: amqplib.ConsumeMessage) => Promise<void>) {
        await this._consumeChannel.consume(queueName, callback);
    }
}