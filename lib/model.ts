import debug from './debug'
import { Schema } from 'tdv'
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
    static AWS = AWS

    protected static _documentClient: DocumentClient
    static get client() {
        return this._documentClient
            || (this._documentClient = new DocumentClient())
    }

    protected static _ddb: DynamoDB
    static get ddb() {
        return this._ddb
            || (this._ddb = new DynamoDB())
    }

    static get tableName(): string {
        return dynamodbFor(this.prototype).tableName || this.name
    }

    static get hashKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tiamo:hash'])
    }

    static get rangeKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tiamo:range'])
    }

    static get globalIndexes() {
        return dynamodbFor(this.prototype).globalIndexes
    }

    static get localIndexes() {
        return dynamodbFor(this.prototype).localIndexes
    }

    static build<M extends Model>(this: ModelStatic<M>, props?: ModelProperties<M>) {
        return new this(props) as M
    }

    static create<M extends Model>(this: ModelStatic<M>, props: ModelProperties<M>, options = {}) {
        return this.build(props).save(options)
    }

    static get<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return this[$get]({ Key }).then(Item => Item && new this(Item) as M)
    }

    static find<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M[]>({ Model: this }),
        )
    }

    static findOne<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key = {}) {
        return Object.keys(Key).reduce(
            (q, k) => q.where(k).eq(Key[k]),
            new Query<M, M>({ Model: this, one: true }),
        )
    }

    static scan<M extends Model>(this: ModelStatic<M>) {
        throw Error('Not implement')
    }

    static update<M extends Model>(this: ModelStatic<M>, Key: DocumentClient.Key) {
        return new Update<M>({ Model: this, Key })
    }

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
    ['constructor']: typeof Model

    constructor(props?) {
        super()
        this.parse(props)
    }

    parse(props = {} as ModelProperties<this>) {
        const { metadata } = this.constructor

        for(const key in metadata) {
            const meta = metadata[key]
            const value = props[key]

            const Ref = meta['tdv:ref']
            const Joi = meta['tdv:joi']

            if (Ref) {
                // skip sub model
                if (!(key in props)) continue

                if (value instanceof Ref || value === null || value === undefined) {
                    // pass sub model instance or null directly
                    this[key] = value
                    continue
                } else if (!this[key]) {
                    // init sub model if not exist
                    this[key] = new Ref()
                }

                if (this[key].parse) {
                    // sub model can parse value
                    this[key].parse(value)
                } else if (value && typeof value === 'object') {
                    // non model class? Schema?
                    Object.assign(this[key], value)
                }
            } else if (Joi) {
                const result = Joi.validate(value)

                if (result.error) {
                    // joi invalid value
                    this[key] = value
                } else {
                    // joi validate will cast trans and set default value
                    this[key] = result.value
                }
            }
        }

        return this
    }

    async save(options?) {
        return this.constructor[$put]({
            Item: this.attempt(),
        }).then(() => this)
    }

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
 * @example
 *      static method<M extends Class>(this: ModelStatic<M>)
 * @see https://github.com/Microsoft/TypeScript/issues/5863#issuecomment-302891200
 */
export type ModelStatic<T> = typeof Model & {
    new(...args): T
    // get(...args): any // IntelliSense still not work :(
}

/**
 * Pick Model non function properties
 * 
 * @example
 * 
 *  class Foo {
 *      constructor(props?: ModelProperties<Foo>) {
 *          super(props) // props: { id: number, name?: string }
 *      }
 *      id: number
 *      name?: string
 *      say() {}
 *  }
 * 
 * @see http://www.typescriptlang.org/docs/handbook/release-notes/typescript-2-8.html
 */
export type ModelProperties<T> = Pick<T, ScalarPropertyNames<T>> & PickModelProperties<T, ModelPropertyNames<T>>
export type ScalarPropertyNames<T> = { [K in keyof T]: T[K] extends Model | Function ? never : K }[keyof T]
export type ModelPropertyNames<T> = { [K in keyof T]: T[K] extends Model ? K : never }[keyof T]
export type PickModelProperties<T, K extends keyof T> = { [P in K]: T[P] | ModelProperties<T[P]> }

// export type ModelProperties<T> = Pick<T, { [K in keyof T]: T[K] extends Function ? never : K }[keyof T]>
