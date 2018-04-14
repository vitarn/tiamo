import debug from './debug'
import './Symbol.asyncIterator'
import { Schema } from 'tdv'
import AWS from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'

const log = debug('db')

export const $put = Symbol.for('put')
export const $get = Symbol.for('get')
export const $query = Symbol.for('query')
export const $scan = Symbol.for('scan')
export const $update = Symbol.for('update')
export const $delete = Symbol.for('delete')
export const $batchGet = Symbol.for('batchGet')
export const $batchWrite = Symbol.for('batchWrite')

const { NODE_ENV } = process.env
const RETURN_CONSUMED_CAPACITY = NODE_ENV === 'production' ? 'NONE' : 'TOTAL'
const RETURN_ITEM_COLLECTION_METRICS = NODE_ENV === 'production' ? 'NONE' : 'SIZE'

export class Database extends Schema {
    /**
     * AWS reference
     */
    static AWS = AWS

    // protected static _ddb: DynamoDB
    /**
     * AWS DynamoDB instance
     */
    // static get ddb() {
    //     return this._ddb
    //         || (this._ddb = new DynamoDB())
    // }
    static ddb = new AWS.DynamoDB()

    // protected static _documentClient: DocumentClient
    /**
     * AWS DynamoDB DocumentClient instance
     */
    // static get client() {
    //     return this._documentClient
    //         || (this._documentClient = new DocumentClient())
    // }
    static client = new DocumentClient()

    /**
     * Configure tiamo to use a DynamoDB local endpoint for testing.
     * 
     * @param endpoint defaults to dynalite 'http://localhost:4567'
     */
    static local(endpoint = 'http://localhost:4567') {
        this.ddb = new AWS.DynamoDB({ endpoint })
        this.client = new DocumentClient({ service: this.ddb })
    }

    /**
     * Table name store in metadata
     */
    static get tableName(): string {
        return Reflect.getOwnMetadata('tiamo:table:name', this) || this.name
    }

    /**
     * HASH key store in metadata
     */
    static get hashKey(): string {
        const key = Reflect.getOwnMetadata('tiamo:table:hash', this.prototype)

        if (!key) throw new Error(`Model ${this.name} missing hash key`)

        return key
    }

    /**
     * RANGE key store in metadata
     */
    static get rangeKey(): string {
        return Reflect.getOwnMetadata('tiamo:table:range', this.prototype)
    }

    /**
     * Global indexes definition store in metadata
     */
    static get globalIndexes() {
        return this.getIndexes()
    }

    /**
     * Local indexes definition store in metadata
     */
    static get localIndexes() {
        return this.getIndexes('local')
    }

    private static getIndexes(scope: 'global' | 'local' = 'global'): {
        [name: string]: {
            hash: string
            range: string
        }
    } {
        const cacheKey = `tiamo:cache:${scope}Indexes`

        if (Reflect.hasOwnMetadata(cacheKey, this)) {
            return Reflect.getOwnMetadata(cacheKey, this)
        }

        const indexes = (Reflect.getMetadataKeys(this.prototype) as string[])
            .reduce((res, key) => {
                if (!key.startsWith(`tiamo:table:index:${scope}:`)) return res

                const [type, name] = key.split(':').reverse()

                res[name] = res[name] || {}
                res[name][type] = Reflect.getMetadata(key, this.prototype)

                return res
            }, {})

        Reflect.defineMetadata(cacheKey, indexes, this)

        return indexes
    }

    static [$batchGet] = async function* (this: DatabaseStatic<Database>, params: DocumentClient.BatchGetItemInput) {
        let { RequestItems, ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY } = params
        let i = 0

        do {
            i++
            const p = { RequestItems, ReturnConsumedCapacity }

            log('⇡ [BATCHGET]#%d request params: %o', i, Object.keys(p.RequestItems).reduce((a, k) => {
                a[k] = p.RequestItems[k].Keys.length
                return a
            }, {}))

            let res = await this.client.batchGet(p).promise()
            if (res.ConsumedCapacity) log('⇣ [BATCHGET]#%d consumed capacity:', i, res.ConsumedCapacity)

            yield res.Responses

            RequestItems = res.UnprocessedKeys
        } while (Object.keys(RequestItems).length) // last time is {}
    }

    static [$batchWrite] = async function* (this: DatabaseStatic<Database>, params: DocumentClient.BatchWriteItemInput) {
        let {
            RequestItems,
            ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY,
            ReturnItemCollectionMetrics = RETURN_ITEM_COLLECTION_METRICS,
        } = params
        let i = 0

        do {
            i++
            const p = { RequestItems, ReturnConsumedCapacity, ReturnItemCollectionMetrics }

            log('⇡ [BATCHWRITE][%d] request...', i)

            let res = await this.client.batchWrite(p).promise()
            if (res.ConsumedCapacity) log('⇣ [BATCHWRITE][%d] consumed capacity:', i, res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [BATCHWRITE][%d] item collection metrics:', i, res.ItemCollectionMetrics)

            yield // what yield from batchWrite?

            RequestItems = res.UnprocessedItems
        } while (Object.keys(RequestItems).length) // last time is {}
    }

    static [$put](params: Partial<DocumentClient.PutItemInput>) {
        const p = { ...params } as DocumentClient.PutItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_OLD'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [PUT] request params:', p)

        return this.client.put(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [PUT] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [PUT] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    static [$get](params: Partial<DocumentClient.GetItemInput>) {
        const p = { ...params } as DocumentClient.GetItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY

        log('⇡ [GET] request params:', p)

        return this.client.get(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [GET] consumed capacity: ', res.ConsumedCapacity)
            return res.Item
        })
    }

    static [$query] = async function* (this: DatabaseStatic<Database>, params: Partial<DocumentClient.QueryInput>) {
        let ExclusiveStartKey
        let { ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY, ...other } = params
        let i = 0

        do {
            i++
            const p = { ...other, ExclusiveStartKey, ReturnConsumedCapacity } as DocumentClient.QueryInput
            p.TableName = p.TableName || this.tableName

            log('⇡ [QUERY]#%d request params: %o', i, p)

            let res = await this.client.query(p).promise()
            if (res.ConsumedCapacity) log('⇣ [QUERY]#%d consumed capacity:', i, res.ConsumedCapacity)

            if (p.Select === 'COUNT') {
                yield res.Count
            } else {
                yield res.Items
            }

            ExclusiveStartKey = res.LastEvaluatedKey
        } while (i == 0 || ExclusiveStartKey)
    }

    static [$scan] = async function* (this: DatabaseStatic<Database>, params: Partial<DocumentClient.ScanInput>) {
        let ExclusiveStartKey
        let { ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY, ...other } = params
        let i = 0

        do {
            i++
            const p = { ...other, ExclusiveStartKey, ReturnConsumedCapacity } as DocumentClient.ScanInput
            p.TableName = p.TableName || this.tableName

            log('⇡ [SCAN]#%d request params: %o', i, p)

            let res = await this.client.scan(p).promise()
            if (res.ConsumedCapacity) log('⇣ [SCAN]#%d consumed capacity:', i, res.ConsumedCapacity)

            if (p.Select === 'COUNT') {
                yield res.Count
            } else {
                yield res.Items
            }

            ExclusiveStartKey = res.LastEvaluatedKey
        } while (i == 0 || ExclusiveStartKey)
    }

    static [$update](params: Partial<DocumentClient.UpdateItemInput>) {
        const p = { ...params } as DocumentClient.UpdateItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_NEW'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [UPDATE] request params:', p)
        return this.client.update(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [UPDATE] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [UPDATE] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    static [$delete](params: Partial<DocumentClient.DeleteItemInput>) {
        const p = { ...params } as DocumentClient.DeleteItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_OLD'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || RETURN_ITEM_COLLECTION_METRICS

        log('⇡ [DELETE] request params:', p)

        return this.client.delete(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [DELETE] consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('⇣ [DELETE] item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }
}

/* TYPES */

/**
 * Static method this type
 * This hack make sub class static method return sub instance
 * But break IntelliSense autocomplete in Typescript 2.7
 * 
 * @example
 * 
 *      static method<T extends Class>(this: Static<T>)
 * 
 * @see https://github.com/Microsoft/TypeScript/issues/5863#issuecomment-302891200
 */
export type DatabaseStatic<T> = typeof Schema & typeof Database & {
    new(...args): T
}
