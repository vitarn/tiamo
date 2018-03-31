import debug from './debug'
import { Schema, SchemaOptions, SchemaProperties } from 'tdv'
import AWS, { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { dynamodbFor } from './metadata'
import { Query } from './query'
import { Update } from './update'
import { Delete } from './delete'

const log = debug('model')

export const $put = Symbol.for('put')
export const $get = Symbol.for('get')
export const $query = Symbol.for('query')
export const $scan = Symbol.for('scan')
export const $update = Symbol.for('update')
export const $delete = Symbol.for('delete')

export class Model extends Schema {
    /**
     * AWS reference
     */
    static AWS = AWS

    protected static _documentClient: DocumentClient
    /**
     * AWS DynamoDB DocumentClient instance
     */
    static get client() {
        return this._documentClient
            || (this._documentClient = new DocumentClient())
    }

    protected static _ddb: DynamoDB
    /**
     * AWS DynamoDB instance
     */
    static get ddb() {
        return this._ddb
            || (this._ddb = new DynamoDB())
    }

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
     * Get item by Key
     */
    static get<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return this[$get]({ Key }).then(Item => Item && new this(Item) as M)
    }

    /**
     * Find items by Key
     */
    static find<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M[]>({ Model: this }),
        )
    }

    /**
     * Find one item by Key
     */
    static findOne<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M>({ Model: this, one: true }),
        )
    }

    /**
     * Scan items
     */
    static scan<M extends Model>(this: ModelStatic<M>) {
        throw Error('Not implement')
    }

    /**
     * Update item by Key
     */
    static update<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Update<M>({ Model: this, Key })
    }

    /**
     * Remove item by Key
     */
    static remove<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Delete<M>({ Model: this, Key })
    }

    static [$put](params: Partial<DocumentClient.PutItemInput>) {
        const p = { ...params } as DocumentClient.PutItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_OLD'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || 'SIZE'

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
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'

        log('⇡ [GET] request params:', p)

        return this.client.get(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [GET] consumed capacity: ', res.ConsumedCapacity)
            return res.Item
        })
    }

    static [$query](params: Partial<DocumentClient.QueryInput>) {
        const p = { ...params } as DocumentClient.QueryInput
        p.TableName = p.TableName || this.tableName
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'

        log('⇡ [QUERY] request params:', p)

        return this.client.query(p).promise().then(res => {
            if (res.ConsumedCapacity) log('⇣ [QUERY] consumed capacity: ', res.ConsumedCapacity)
            return res.Items
        })
    }

    static [$update](params: Partial<DocumentClient.UpdateItemInput>) {
        const p = { ...params } as DocumentClient.UpdateItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_NEW'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || 'SIZE'

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
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || 'SIZE'

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
     * Remove from db by Key
     */
    remove(options?) {
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
