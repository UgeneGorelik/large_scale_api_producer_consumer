#!/usr/bin/env node
import { Config } from "./config";
import { StreetsService, Street } from "./israelistreets/StreetsService";
import { PostgresService } from "./postgresService/postgres";
import { RabbitmqService } from "./rabbitService/rmq";
import { RedpandaService } from "./redpandaService/redpanda";
import { MongoService } from "./mongoService/mongo";
import pLimit from "p-limit";
import { LRUCache } from "lru-cache";

// Type for tracking data paired with its source message for ACKing
interface BatchItem {
    data: Street;
    message: any;
}

export async function consume(broker: string) {
    const isRedpanda = broker === "red";
    const brokerService = isRedpanda ? await RedpandaService.init() : await RabbitmqService.init();
    const pgService = await PostgresService.init();
    const mongoService = await MongoService.init();

    const CONCURRENCY = 65535;
    const DB_BATCH_SIZE = 1000;
    const STREET_BATCH_SIZE = 100;
    const FLUSH_TIMEOUT = 1000;
    const DLQ_NAME = `${Config.rabbitMq.queueConfig.queue}_dlq`;

    const limit = pLimit(CONCURRENCY);

    if (!isRedpanda && "consumeChannel" in brokerService) {
        brokerService.consumeChannel.prefetch(CONCURRENCY);
        await brokerService.consumeChannel.assertQueue(DLQ_NAME, { durable: true });
    }

    // ------------------ DLQ Logic ------------------
    const sendToDLQ = async (message: any, error: string) => {
        console.error(`[DLQ] Sending message to ${DLQ_NAME}. Reason: ${error}`);
        if (!isRedpanda && "consumeChannel" in brokerService) {
            const content = message.content;
            brokerService.consumeChannel.sendToQueue(DLQ_NAME, content, { headers: { x_error: error } });
            brokerService.consumeChannel.ack(message);
        }
    };

    // ------------------ DB Batching & Integrity ------------------
    let mongoBuffer: BatchItem[] = [];
    let pgBuffer: BatchItem[] = [];
    let dbFlushTimeout: NodeJS.Timeout | null = null;
    let isFlushing = false;

    async function flushDbBatches() {
        if (isFlushing) return;
        isFlushing = true;

        if (dbFlushTimeout) { clearTimeout(dbFlushTimeout); dbFlushTimeout = null; }

        // Atomic swap: move items to local variables and clear global buffers immediately
        const mongoToProcess = [...mongoBuffer];
        const pgToProcess = [...pgBuffer];
        mongoBuffer = [];
        pgBuffer = [];

        try {
            // Process MongoDB
            if (mongoToProcess.length) {
                try {
                    await mongoService.insertMany(mongoToProcess.map(i => i.data));
                    if (!isRedpanda && "consumeChannel" in brokerService) {
                        mongoToProcess.forEach(item => brokerService.consumeChannel.ack(item.message));
                    }
                    console.log(`[DB] Flushed ${mongoToProcess.length} to MongoDB.`);
                } catch (err) {
                    console.error("[DB] Mongo Batch Error:", err);
                    // Nack and return to queue
                    mongoToProcess.forEach(item => {
                        if (!isRedpanda) brokerService.consumeChannel.nack(item.message, false, true);
                    });
                }
            }

            // Process Postgres
            if (pgToProcess.length) {
                try {
                    const table = Config.postgres.dbConfig.streetsTableName;
                    const columns = `street_id, region_code, region_name, city_code, city_name, street_code, street_name, street_name_status, official_code`;
                    const values = pgToProcess.flatMap(item => {
                        const s = item.data;
                        return [s.streetId, s.region_code, s.region_name, s.city_code, s.city_name, s.street_code, s.street_name, s.street_name_status, s.official_code];
                    });

                    const placeholders = pgToProcess.map((_, i) =>
                        `($${i * 9 + 1},$${i * 9 + 2},$${i * 9 + 3},$${i * 9 + 4},$${i * 9 + 5},$${i * 9 + 6},$${i * 9 + 7},$${i * 9 + 8},$${i * 9 + 9})`
                    ).join(",");

                    await pgService.query(`INSERT INTO ${table} (${columns}) VALUES ${placeholders}`, values);

                    if (!isRedpanda && "consumeChannel" in brokerService) {
                        pgToProcess.forEach(item => brokerService.consumeChannel.ack(item.message));
                    }
                    console.log(`[DB] Flushed ${pgToProcess.length} to Postgres.`);
                } catch (err) {
                    console.error("[DB] Postgres Batch Error:", err);
                    pgToProcess.forEach(item => {
                        if (!isRedpanda) brokerService.consumeChannel.nack(item.message, false, true);
                    });
                }
            }
        } finally {
            isFlushing = false;
            // Check if a new batch filled up while we were awaiting the DB
            if (mongoBuffer.length >= DB_BATCH_SIZE || pgBuffer.length >= DB_BATCH_SIZE) {
                void flushDbBatches();
            }
        }
    }

    // ------------------ Street Info Cache + Batching ------------------
    const streetCache = new LRUCache<number, Street>({
        max: 200_000,
        ttl: 24 * 60 * 60 * 1000,
    });

    let streetIds: number[] = [];
    let resolvers: ((s: Street) => void)[] = [];
    let rejectors: ((e: any) => void)[] = [];
    let streetFlushTimeout: NodeJS.Timeout | null = null;

    async function flushStreetBatch() {
        if (streetFlushTimeout) { clearTimeout(streetFlushTimeout); streetFlushTimeout = null; }
        if (!streetIds.length) return;

        const ids = [...streetIds];
        const res = [...resolvers];
        const rej = [...rejectors];
        streetIds = []; resolvers = []; rejectors = [];

        try {
            const uncachedIds = ids.filter(id => !streetCache.has(id));
            if (uncachedIds.length) {
                const fetchedStreets = await StreetsService.getStreetInfoByIds(uncachedIds);
                fetchedStreets.forEach(s => streetCache.set(s.streetId, s));
            }

            ids.forEach((id, i) => {
                const street = streetCache.get(id);
                street ? res[i](street) : rej[i](new Error(`ID ${id} missing in API`));
            });
        } catch (err) {
            rej.forEach(r => r(err));
        }
    }

    function getStreetBatched(id: number): Promise<Street> {
        const cached = streetCache.get(id);
        if (cached) return Promise.resolve(cached);

        return new Promise((resolve, reject) => {
            streetIds.push(id);
            resolvers.push(resolve);
            rejectors.push(reject);

            if (streetIds.length >= STREET_BATCH_SIZE) {
                void flushStreetBatch();
            } else if (!streetFlushTimeout) {
                streetFlushTimeout = setTimeout(() => void flushStreetBatch(), 200);
            }
        });
    }

    // ------------------ Main Consumer ------------------
    const queue = Config.rabbitMq.queueConfig.queue;
    console.log(`[System] Consumer started. Concurrency: ${CONCURRENCY}, Queue: ${queue}`);

    await brokerService.subscribe(queue, async (message: any) => {
        limit(async () => {
            const raw = isRedpanda ? message.content : message.content.toString();
            try {
                const data = JSON.parse(raw);
                const street = await getStreetBatched(data.streetId);

                if (data.db_type === "mongo") {
                    mongoBuffer.push({ data: street, message });
                } else {
                    pgBuffer.push({ data: street, message });
                }

                if (mongoBuffer.length >= DB_BATCH_SIZE || pgBuffer.length >= DB_BATCH_SIZE) {
                    await flushDbBatches();
                } else if (!dbFlushTimeout) {
                    dbFlushTimeout = setTimeout(() => void flushDbBatches(), FLUSH_TIMEOUT);
                }
            } catch (err: any) {
                await sendToDLQ(message, err.message || "Logic Error");
            }
        });
    });

    // ------------------ Graceful Shutdown ------------------
    const shutdown = async () => {
        console.log("\n[System] SIGTERM/SIGINT received. Cleaning up buffers...");

        // Disable incoming processing if possible (Broker-specific)
        // Then, flush everything left in RAM
        await flushStreetBatch();

        // Manual final flush bypassing the 'isFlushing' check to ensure exit
        if (mongoBuffer.length || pgBuffer.length) {
            console.log("[System] Final DB flush...");
            isFlushing = false; // Force unlock
            await flushDbBatches();
        }

        console.log("[System] Cleanup complete. Goodbye.");
        process.exit(0);
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}

const brokerArg = (process.argv[2] || "rabbit").toLowerCase();
consume(brokerArg).catch(console.error);