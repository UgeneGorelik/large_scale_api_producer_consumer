const { MongoClient } = require("mongodb");
const { Config } = require("../config"); // ✅ add .js

export class MongoService {
    private _mongo: MongoClient;

    constructor(mongo: MongoClient) {
        this._mongo = mongo;
    }

    /** Initialize Mongo connection */
    static async init() {
        try {
            console.log("Connecting to MongoDB:", Config.mongo.connectionString);
            const _mongo = await MongoClient.connect(Config.mongo.connectionString);
            return new MongoService(_mongo);
        } catch (err) {
            console.error(`Failed connecting to Mongo: ${err}`);
            throw err;
        }
    }

    /** Insert single document with retry */
    async insert<T>(collectionName: string, document: T, maxRetries = 5) {
        const db = this._mongo.db(Config.mongo.connection.db);
        const collection = db.collection(collectionName);
        let attempt = 0;
        let delay = 500;

        while (attempt < maxRetries) {
            try {
                return await collection.insertOne(document as any);
            } catch (err) {
                attempt++;
                if (attempt >= maxRetries) throw err;
                console.warn(`[Mongo] Insert attempt ${attempt} failed. Retrying in ${delay}ms...`);
                await new Promise(res => setTimeout(res, delay));
                delay *= 2;
            }
        }
    }

    /** Batch insert with retry */
    async insertMany<T>( documents: T[], maxRetries = 5) {
        if (documents.length === 0) return;
        const db = this._mongo.db(Config.mongo.connection.db);
        const collection = db.collection(Config.mongo.collectionName);
        let attempt = 0;
        let delay = 500;

        while (attempt < maxRetries) {
            try {
                return await collection.insertMany(documents as any[]);
            } catch (err) {
                attempt++;
                if (attempt >= maxRetries) {
                    console.error(`[Mongo] Batch insert failed after ${maxRetries} attempts:`, err);
                    throw err;
                }
                console.warn(`[Mongo] Batch insert attempt ${attempt} failed. Retrying in ${delay}ms...`);
                await new Promise(res => setTimeout(res, delay));
                delay *= 2;
            }
        }
    }
}