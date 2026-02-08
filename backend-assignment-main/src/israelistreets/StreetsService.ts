#!/usr/bin/env node
import axios, { Axios } from 'axios';
import { omit } from 'lodash';
import { cities, city, englishNameByCity } from './cities';
import pThrottle from 'p-throttle';

export interface Street extends Omit<ApiStreet, '_id'> {
    streetId: number;
}

interface ApiStreet {
    _id: number;
    region_code: number;
    region_name: string;
    city_code: number;
    city_name: string;
    street_code: number;
    street_name: string;
    street_name_status: string;
    official_code: number;
}

const throttle = pThrottle({
    limit: 10000000,
    interval: 1000000
});

export class StreetsService {
    private static _axios: Axios;

    private static get axios() {
        if (!this._axios) {
            this._axios = axios.create({
                timeout: 15_000,
                headers: { 'User-Agent': 'datagov-external-client' }
            });
        }
        return this._axios;
    }

    private static async retryRequest<T>(fn: () => Promise<T>, retries = 3, delayMs = 2000): Promise<T> {
        let attempt = 0;
        while (true) {
            try {
                return await fn();
            } catch (err: any) {
                attempt++;
                const status = err.response?.status;
                const shouldRetry = attempt <= retries && (!status || [429, 502, 503, 504].includes(status));
                if (!shouldRetry) throw err;
                console.warn(`[API] Retry ${attempt} for status ${status}...`);
                await new Promise(r => setTimeout(r, delayMs));
                delayMs *= 2;
            }
        }
    }

    /**
     * ✅ MEMORY SAFE: Async Generator for large citzies
     */
    static async *streamStreetsInCity(cityName: city) {
        let offset = 0;
        const CHUNK_SIZE = 10000;
        let hasMore = true;

        while (hasMore) {
            const res = await this.retryRequest(async () => {
                const response = await this.axios.post('fill in api',
                    filters: { city_name: cities[cityName] },
                    limit: CHUNK_SIZE,
                    offset: offset
                });
                return response.data;
            });

            const records = res.result.records;
            if (!records || records.length === 0) {
                hasMore = false;
                break;
            }

            for (const record of records) {
                yield {
                    streetId: record._id,
                    street_name: record.street_name.trim()
                };
            }

            offset += CHUNK_SIZE;
            if (records.length < CHUNK_SIZE) hasMore = false;
        }
    }

    static getStreetInfoByIds = throttle(async (ids: number[]): Promise<Street[]> => {
        if (ids.length === 0) return [];
        const res = await StreetsService.retryRequest(async () => {
            const response = await StreetsService.axios.post('fill in api',
                filters: { _id: ids },
                limit: ids.length
            });
            return response.data;
        });

        return (res.result?.records || []).map((dbStreet: ApiStreet) => ({
            ...omit(dbStreet, '_id'),
            streetId: dbStreet._id,
            city_name: englishNameByCity[dbStreet.city_name] || dbStreet.city_name,
            region_name: dbStreet.region_name?.trim() || '',
            street_name: dbStreet.street_name?.trim() || ''
        }));
    });
}