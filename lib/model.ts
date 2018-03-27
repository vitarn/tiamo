import debug from './debug'
import { Schema } from 'tdv'
import AWS, { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { dynamodbFor } from './metadata'
import { Query } from './query'
import { Update } from './update'

const log = debug('model')

export class Model extends Schema {
    static AWS = AWS

    static _documentClient
    static get client(): DocumentClient {
        return this._documentClient
            || (this._documentClient = new DocumentClient())
    }

    static _ddb
    static get ddb() {
        return this._ddb
            || (this._ddb = new DynamoDB())
    }

    static get tableName() {
        return dynamodbFor(this.prototype).tableName || this.name
    }

    static get hashKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tdmo:hash'])
    }

    static get rangeKey() {
        const { metadata } = this
        return Object.keys(metadata).find(key => metadata[key]['tdmo:range'])
    }

    static get globalIndexes() {
        const { metadata } = this
        return Object.keys(metadata).filter(key => metadata[key]['tdmo:index'] && metadata[key]['tdmo:index'].global)
    }

    static get localIndexes() {
        const { metadata } = this
        return Object.keys(metadata).filter(key => metadata[key]['tdmo:index'] && !metadata[key]['tdmo:index'].global)
    }

    static async create<M extends Model>(obj, options = {}) {
        return (<M>new this(obj)).save(options)
    }

    static async findById<M extends Model>(Key) {
        return this._get({ Key }).then(Item => <M>new this(Item))
    }

    static find<M extends Model>() {
        return new Query<M, M[]>({ Model: this })
    }

    static findOne<M extends Model>() {
        return new Query<M, M>({ Model: this, one: true })
    }

    static update<M extends Model>(Key: DocumentClient.Key) {
        return new Update<M>({ Model: this, Key })
    }

    static async _put(params: Partial<DocumentClient.PutItemInput>) {
        const p = { ...params } as DocumentClient.PutItemInput
        p.TableName = p.TableName || this.tableName
        return this.client.put(p).promise()
    }

    static async _get(params: Partial<DocumentClient.GetItemInput>) {
        const p = { ...params } as DocumentClient.GetItemInput
        p.TableName = p.TableName || this.tableName
        return this.client.get(p).promise().then(res => res.Item)
    }

    static async _query(params: Partial<DocumentClient.QueryInput>) {
        const p = { ...params } as DocumentClient.QueryInput
        p.TableName = p.TableName || this.tableName
        return this.client.query(p).promise().then(res => res.Items)
    }

    static async _update(params: Partial<DocumentClient.UpdateItemInput>) {
        const p = { ...params } as DocumentClient.UpdateItemInput
        p.TableName = p.TableName || this.tableName
        p.ReturnValues = p.ReturnValues || 'ALL_NEW'
        p.ReturnConsumedCapacity = p.ReturnConsumedCapacity = 'TOTAL'
        p.ReturnItemCollectionMetrics = p.ReturnItemCollectionMetrics || 'SIZE'

        log('update params', p)
        return this.client.update(p).promise().then(res => {
            // log('update response', res.$response)
            if (res.ConsumedCapacity) log('update consumed capacity: ', res.ConsumedCapacity)
            if (res.ItemCollectionMetrics) log('update item collection metrics: ', res.ItemCollectionMetrics)
            return res.Attributes
        })
    }

    static async _delete(params: Partial<DocumentClient.DeleteItemInput>) {
        const p = { ...params } as DocumentClient.DeleteItemInput
        p.TableName = p.TableName || this.tableName
        return this.client.delete(p).promise().then(res => res.Attributes)
    }

    /**
     * @see https://github.com/Microsoft/TypeScript/issues/3841#issuecomment-337560146
     */
    ['constructor']: typeof Model

    constructor(props?) {
        super(props)
    }

    async save(options?) {
        return this.constructor._put({
            Item: this.attempt(),
        }).then(() => this)
    }

    // async remove(options?) {
    //     return this.constructor.client.delete({
    //         TableName: this.constructor.tableName,
    //         Key:
    //     }).promise()
    // }
}
