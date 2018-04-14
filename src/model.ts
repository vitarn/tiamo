import debug from './debug'
import './Symbol.asyncIterator'
import { Schema, SchemaOptions, SchemaProperties, SchemaValidationOptions } from 'tdv'
import { ValidationResult } from 'joi'
import AWS from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Database } from './database'
import { Hook } from './hook'
import { Put } from './put'
import { Get } from './get'
import { Query } from './query'
import { Scan } from './scan'
import { Update } from './update'
import { Delete } from './delete'
import { BatchGet } from './batchGet'
import { BatchWrite } from './batchWrite'

export * from './database'

const log = debug('model')

export const $hook = Symbol.for('hook')

export class Model extends Database {
    /**
     * Timestamps definition store in metadata
     */

    static get timestamps() {
        const cacheKey = 'tiamo:cache:timestamps'

        if (Reflect.hasOwnMetadata(cacheKey, this)) {
            return Reflect.getOwnMetadata(cacheKey, this)
        }

        const timestamps = {}

        for (let type of ['create', 'update', 'expire']) {
            const key = `tiamo:timestamp:${type}`
            if (Reflect.hasMetadata(key, this.prototype)) {
                timestamps[type] = Reflect.getMetadata(key, this.prototype)
            }
        }

        Reflect.defineMetadata(cacheKey, timestamps, this)

        return timestamps
    }

    static init<M extends Model>(this: ModelStatic<M>, props: SchemaProperties<M>, options = {} as ModelOptions) {
        const { isNew, ...other } = options

        return super.build(props, { ...other, convert: false }) as M
    }

    /**
     * Create model instance. Build and put but not overwrite existed one.
     */
    static async create<M extends Model>(this: ModelStatic<M>, props: SchemaProperties<M>, options = {} as ModelOptions) {
        const Item = (this.build(props, { convert: false }) as M).validate({ raise: true }).value

        const put = this.put(Item).where(this.hashKey).not.exists()
        if (this.rangeKey) put.where(this.rangeKey).not.exists()

        return put
    }

    /**
     * Put item into db
     */
    static put<M extends Model>(this: ModelStatic<M>, Item: DocumentClient.PutItemInputAttributeMap) {
        return new Put<M>({ Model: this, Item })
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
        return new Scan<M>({ Model: this })
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

    /**
     * @see https://github.com/Microsoft/TypeScript/issues/3841#issuecomment-337560146
     */
    ['constructor']: ModelStatic<this>

    constructor(props?, options = {} as ModelOptions) {
        super(props, options)

        if (options.isNew !== false) {
            Reflect.defineMetadata('tiamo:cache:new', true, this)
        }

        this.pre('save', this.validate.bind(this))
    }

    get isNew() {
        return !!Reflect.getOwnMetadata('tiamo:cache:new', this)
    }
    set isNew(value: boolean) {
        Reflect.defineMetadata('tiamo:cache:new', value, this)
    }

    /**
     * Hook instance
     */
    protected get [$hook]() {
        if (Reflect.hasOwnMetadata('tiamo:cache:hook', this)) {
            return Reflect.getOwnMetadata('tiamo:cache:hook', this) as Hook
        }

        const hook = new Hook()

        Reflect.defineMetadata('tiamo:cache:hook', hook, this)

        return hook
    }

    /**
     * Pre hook
     */
    pre(name: string, fn: Function) {
        this[$hook].pre(name, fn)

        return this
    }

    /**
     * Post hook
     */
    post(name: string, fn: Function) {
        this[$hook].post(name, fn)

        return this
    }

    /**
     * Validate by Joi
     * 
     * * Be careful the value returned is a new instance. This is design by Joi.
     * * We strip unknown properties so you can put your fields safely.
     */
    validate(options = {} as SchemaValidationOptions) {
        return this._validate(options)
    }
    protected get _validate() {
        return this[$hook].wrap('validate', (options = {} as SchemaValidationOptions) => {
            return super.validate({
                // when true, ignores unknown keys with a function value. Defaults to false.
                skipFunctions: true,
                // when true, all unknown elements will be removed.
                stripUnknown: true,
                ...options,
            })
        })
    }

    /**
     * Save into db
     * 
     * Validate before save. Throw `ValidateError` if invalid. Apply casting and default if valid.
     */
    // async save(options?) {
    //     return this.constructor.put({
    //         Item: this.validate({ apply: true, raise: true }).value,
    //     }).then(() => this)
    // }
    get save() {
        return this[$hook].wrap('save', (options?) => {
            const Item = this.validate({ apply: true, raise: true }).value
            const { hashKey, rangeKey } = this.constructor
            const put = this.constructor.put(Item).where(hashKey).not.exists()
            if (rangeKey) put.where(rangeKey).not.exists()

            return put
        })
    }

    /**
     * Delete from db by key
     */
    delete(options?) {
        const { hashKey, rangeKey } = this.constructor
        const Key: DocumentClient.Key = { [hashKey]: this[hashKey] }
        if (rangeKey) Key[rangeKey] = this[rangeKey]
        
        return this.constructor.delete(Key)
            .then(() => this)
    }
}

/* TYPES */

/**
 * Model static method this type
 * This hack make sub class static method return sub instance
 * But break IntelliSense autocomplete in Typescript 2.7
 * 
 * @example
 * 
 *      static method<T extends Class>(this: Static<T>)
 * 
 * @see https://github.com/Microsoft/TypeScript/issues/5863#issuecomment-302891200
 */
export type ModelStatic<T> = typeof Schema & typeof Database & typeof Model & {
    new(...args): T
}

export interface ModelOptions extends SchemaOptions {
    isNew?: boolean
}
