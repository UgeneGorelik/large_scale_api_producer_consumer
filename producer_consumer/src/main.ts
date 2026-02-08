#!/usr/bin/env node

import { StreetsService } from "./israelistreets/StreetsService";
import { RabbitmqService } from "./rabbitService/rmq";
import { RedpandaService } from "./redpandaService/redpanda";
import { city, cities } from "./israelistreets/cities"; // your dictionary
import pLimit from "p-limit";
import pThrottle from "p-throttle";

/* ================== CONFIG ================== */

const CONCURRENCY = 100;        // streets concurrency per city
const CITY_CONCURRENCY = 2;     // how many cities in parallel
const MAX_RETRIES = 5;

// rate limit: max N publishes per interval
const throttle = pThrottle({
    limit: 20000,
    interval: 1000
});

/* ================== PUBLISH WITH RETRY ================== */

async function publishWithRetry(
    service: any,
    streetId: number,
    db_type: string
) {
    let attempt = 0;
    let delay = 500;

    while (attempt <= MAX_RETRIES) {
        try {
            const throttledPublish = throttle(() =>
                service.publish({ streetId, db_type })
            );

            await throttledPublish();
            return;
        } catch (err) {
            attempt++;

            if (attempt > MAX_RETRIES) {
                console.error(
                    `[ERROR] Permanent failure for streetId ${streetId}`,
                    err
                );
                return;
            }

            await new Promise(r => setTimeout(r, delay));
            delay *= 2; // exponential backoff
        }
    }
}

/* ================== PROCESS ONE CITY ================== */

const processCity = async (
    service: any,
    cityName: city,
    db_type: string
) => {
    console.log(`[*] Processing city: ${cityName}`);

    const limit = pLimit(CONCURRENCY);
    const tasks: Promise<void>[] = [];
    let count = 0;

    for await (const street of StreetsService.streamStreetsInCity(cityName)) {
        const task = limit(async () => {
            await publishWithRetry(service, street.streetId, db_type);
            count++;
        });

        tasks.push(task);

        if (tasks.length >= 2000) {
            await Promise.all(tasks);
            tasks.length = 0;
            console.log(`[${cityName}] Published ${count} streets`);
        }
    }

    await Promise.all(tasks);
    console.log(`[SUCCESS] ${cityName}: Published ${count} streets`);
};

/* ================== MAIN ================== */

const main = async (
    citiesToProcess: city[],
    db_type: string,
    useRedpanda: boolean
) => {
    let service: any;

    try {
        service = useRedpanda
            ? await RedpandaService.init()
            : await RabbitmqService.init();

        // Filter out cities that don't exist in the dictionary
        const validCities = citiesToProcess.filter(cityName => {
            if (!(cityName in cities)) {
                console.warn(`[WARN] City "${cityName}" not found in dictionary. Skipping.`);
                return false;
            }
            return true;
        });

        if (validCities.length === 0) {
            console.log("[INFO] No valid cities to process. Exiting.");
            return;
        }

        console.log(`[*] Starting processing for ${validCities.length} valid cities`);

        const cityLimit = pLimit(CITY_CONCURRENCY);

        await Promise.all(
            validCities.map(cityName =>
                cityLimit(() =>
                    processCity(service, cityName, db_type)
                )
            )
        );

        console.log("[SUCCESS] All valid cities processed");

    } catch (error) {
        console.error("[FATAL] Main process error:", error);
        process.exit(1);
    } finally {
        console.log("[*] Shutting down...");
        await service?.close?.();
    }
};

/* ================== CLI ================== */

const requestedCities = process.argv.slice(2) as city[];

if (requestedCities.length === 0) {
    console.error(
        'Usage: npx ts-node main.ts "eshbol" "benei dkalim"'
    );
    process.exit(1);
}

const db_type = "pg";
const useRedpanda = false;

main(requestedCities, db_type, useRedpanda)
    .then(() => process.exit(0));
