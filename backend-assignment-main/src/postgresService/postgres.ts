import { Pool } from 'pg'
import { omit } from 'lodash'
import { Config } from '../config'

export class PostgresService{
  static _pool: Pool
  private static async _initializeDB(): Promise<Pool>{
    const admin = await new Pool(omit(Config.postgres.connection, 'database')).connect()
    try{
      await admin.query(`Create database ${Config.postgres.dbConfig.dbName}`)
    }catch(error){
      if (error?.code === '42P04' && error?.message?.includes('already exists')){
        console.log(`Database ${Config.postgres.dbConfig.dbName} already exists, continuing`)
      }else{
        console.error(error)
        throw error
      }
    }
    const pool = new Pool(
      Config.postgres.connection
    )
    await pool.connect()
    await pool.query(`Create table if not exists ${Config.postgres.dbConfig.streetsTableName}(
      street_id INT,
      region_code INT,
      region_name TEXT,
      city_code INT,
      city_name TEXT,
      street_code INT,
      street_name TEXT,
      street_name_status TEXT,
      official_code  INT
    )`)
    return pool
  }
  static async init(): Promise<PostgresService>{
    if (!this._pool){
      this._pool = await this._initializeDB()
      return new PostgresService(this._pool)
    }else{
      return new PostgresService(this._pool)
    }
  }

  constructor(
    public pool: Pool
  ){}

  async query (text: any, params?: any){
    try {
      return await this.pool.query(text, params)
    } catch (error) {
      console.error(`Error executing query: ${text}, ${params}, error: ${error}`)
      throw error
    }
  }
}