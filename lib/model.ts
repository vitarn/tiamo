import debug from './debug'
import { Schema, SchemaOptions, SchemaProperties } from 'tdv'
import AWS, { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { dynamodbFor } from './metadata'
import { BatchGet } from './batchGet'
import { BatchWrite } from './batchWrite'
import { Get } from './get'
import { Query } from './query'
import { Update } from './update'
import { Delete } from './delete'
import './Symbol.asyncIterator'

const log = debug('model')

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

export class Model extends Schema {
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
     * Table name store in metadata
     */
    static get tableName(): string {
        return dynamodbFor(this.prototype).tableName || this.name
    }

    /**
     * HASH key store in metadata
     */
    static get hashKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tiamo:hash'])
    }

    /**
     * RANGE key store in metadata
     */
    static get rangeKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tiamo:range'])
    }

    /**
     * Global indexes definition store in metadata
     */
    static get globalIndexes() {
        return dynamodbFor(this.prototype).globalIndexes
    }

    /**
     * Local indexes definition store in metadata
     */
    static get localIndexes() {
        return dynamodbFor(this.prototype).localIndexes
    }

    /**
     * Create model instance. Build and save.
     */
    static create<M extends Model>(this: ModelStatic<M>, props: SchemaProperties<M>, options = {}) {
        return (this.build(props, { convert: false }) as M).save(options)
    }

    /**
     * Get item by key
     */
    static get<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Get<M>({ Model: this, Key })
    }

    /**
     * Query items by key
     */
    static query<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M[]>({ Model: this }),
        )
    }

    /**
     * Scan items
     */
    static scan<M extends Model>(this: ModelStatic<M>) {
        throw Error('Not implement')
    }

    /**
     * Update item by key
     */
    static update<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Update<M>({ Model: this, Key })
    }

    /**
     * Delete item by key
     */
    static delete<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Delete<M>({ Model: this, Key })
    }

    /**
     * Batch operate
     * 
     * * Chain call `put` and `delete`
     * * One way switch context from `put` or `delete` to `get`
     * * Operate order `put` -> `delete` -> `get` -> return
     * 
     * @return PromiseLike or AsyncIterable
     * 
     * @example
     * 
     *      // get only
     *      Model.batch().get({})
     *      // write only
     *      Model.batch().put({})
     *      // chain
     *      Model.batch().put({}).delete({}).get({})
     *      // async interator
     *      for await (let m of Model.batch().get([])) {
     *          console.log(m.id)
     *      }
     */
    static batch<M extends Model>(this: ModelStatic<M>) {
        const self = this

        return {
            /**
             * Batch get
             * 
             * @example
             * 
             *      Model.batch().get({ id: 1 })
             */
            get(...GetKeys: DocumentClient.KeyList) {
                return new BatchGet<M>({ Model: self, GetKeys })
            },
            /**
             * Batch write put request
             * 
             * @example
             * 
             *      Model.batch().put({ id: 1, name: 'tiamo' })
             */
            put(...PutItems: DocumentClient.PutItemInputAttributeMap[]) {
                return new BatchWrite<M>({ Model: self, PutItems })
            },
            /**
             * Batch write delete request
             * 
             * @example
             * 
             *      Model.batch().delete({ id: 1 })
             */
            delete(...DeleteKeys: DocumentClient.KeyList) {
                return new BatchWrite<M>({ Model: self, DeleteKeys })
            },
        }
    }

    static [$batchGet] = async function* (this: ModelStatic<Model>, params: DocumentClient.BatchGetItemInput) {
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

    static [$batchWrite] = async function* (this: ModelStatic<Model>, params: DocumentClient.BatchWriteItemInput) {
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

    static [$query](params: Partial<DocumentClient.QueryInput>) {
        const p = { ...params } as DocumentClient.QueryInput
        p.TableName = p.TableName || this.tableName
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = RETURN_CONSUMED_CAPACITY

        log('⇡ [QUERY] request params:', p)

        return this.client.query(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [QUERY] consumed capacity: ', res.ConsumedCapacity)
            if (p.Select === 'COUNT') {
                return res.Count
            } else {
                return res.Items
            }
        })
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

    /**
     * @see https://github.com/Microsoft/TypeScript/issues/3841#issuecomment-337560146
     */
    ['constructor']: ModelStatic<this>

    constructor(props?, options?: SchemaOptions) {
        super(props, options)
    }

    /**
     * Save into db
     * 
     * Validate before save. Throw `ValidateError` if invalid. Apply casting and default if valid.
     */
    async save(options?) {
        return this.constructor[$put]({
            Item: this.validate({ apply: true, raise: true }).value,
        }).then(() => this)
    }

    /**
     * Delete from db by key
     */
    delete(options?) {
        const { hashKey, rangeKey } = this.constructor
        const Key: DocumentClient.Key = { [hashKey]: this[hashKey] }
        if (rangeKey) Key[rangeKey] = this[rangeKey]
        return this.constructor[$delete]({ Key })
    }
}

/* TYPES */

/**
 * Model static method this type
 * This hack make sub class static method return sub instance
 * But break IntelliSense autocomplete in Typescript@2.7
 * 
 * @example
 * 
 *      static method<M extends Class>(this: ModelStatic<M>)
 * 
 * @see https://github.com/Microsoft/TypeScript/issues/5863#issuecomment-302891200
 */
export type ModelStatic<T> = typeof Model & {
    new(...args): T
    // get(...args): any // IntelliSense still not work :(
}
