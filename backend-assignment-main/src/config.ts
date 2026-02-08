import type { Options } from "amqplib/properties"
import { PoolConfig } from "pg"

export interface MongoConfig {
    hostname: string
    prefix: string
    db: string
}

export const Config = {
    postgres: {
        get connection(): PoolConfig {
            return {
                host: process.env.POSTGRES_HOST || 'localhost',
                user: process.env.POSTGRES_USER || 'postgres',
                password: process.env.POSTGRES_PASSWORD || 'password',
                port: +(process.env.POSTGRES_HOST || 5433),
                database: this.dbConfig.dbName
            }
        },
        get dbConfig() {
            return {
                dbName: process.env.DB_NAME || 'streets',
                streetsTableName: process.env.STREETS_TABLE_NAME || 'streets'
            }
        }
    },
    rabbitMq: {
        get connection(): Options.Connect {
            return {
                hostname: process.env.RABBIT_HOST || 'localhost',
                username: process.env.RABBIT_USER || 'guest',
                password: process.env.RABBIT_PASSWORD || 'guest'
            }
        },
        get queueConfig() {
            return {
                queue: process.env.RABBIT_QUEUE_NAME ||  'streets.queue',
                exchange: process.env.RABBIT_EXCHANGE_NAME || 'streets.exchange'
            }
        },
        get prefetchCount(): number{
            return (+process.env.RABBIT_PREFETCH_COUNT || 10)
        }
    },
    mongo: {
        get connection(): MongoConfig{
            return {
                hostname: process.env.MONGO_HOST || 'localhost',
                prefix: process.env.MONGO_PREFIX || 'mongodb',
                db: process.env.MONGO_DATABASE || 'streets',
            }
        },
        get connectionString(): string{
            const {prefix, hostname, db} = this.connection
            return `${prefix}://${hostname}/${db}`
        },
        get collectionName(): string{
            return process.env.MONGO_STREETS_COLLECTION || 'streets'
        }
    }

} as const