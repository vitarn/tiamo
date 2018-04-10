import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $batchWrite } from './model'
import { expression } from './expression'
import { BatchGet } from './batchGet'

export class BatchWrite<M extends Model> implements AsyncIterable<M> {
    // legacy: AttributesToGet
    static PARAM_KEYS = `
        PutItems DeleteKeys
        ReturnConsumedCapacity
        ReturnItemCollectionMetrics
    `.split(/\s+/)

    constructor(private options = {} as BatchWriteOptions<M>) {
        options = { ...options }
        this.options = options

        options.PutItems = options.PutItems || []
        options.DeleteKeys = options.DeleteKeys || []
    }

    put(...PutItems: DocumentClient.PutItemInputAttributeMap[]) {
        this.options.PutItems.push(...PutItems)

        return this
    }

    delete(...DeleteKeys: DocumentClient.KeyList) {
        this.options.DeleteKeys.push(...DeleteKeys)

        return this
    }

    get(GetKeys: DocumentClient.KeyList) {
        const { Model } = this.options

        return new BatchGet<M>({ Model, GetKeys, batchWrite: this })
    }

    quiet() {
        this.options.ReturnConsumedCapacity = 'NONE'
        this.options.ReturnItemCollectionMetrics = 'NONE'

        return this
    }

    inspect() {
        return { BatchWrite: this.toJSON() }
    }

    toJSON() {
        const { options } = this
        const { Model, PutItems, DeleteKeys, ...other } = options
        const { tableName } = Model
        const json = {
            RequestItems: {
                [tableName]: []
            }
        } as DocumentClient.BatchWriteItemInput

        json.RequestItems[tableName].push(
            ...PutItems.reduce((a, v) => a.concat(v), []).map(Item => ({ PutRequest: { Item } })),
            ...DeleteKeys.reduce((a, v) => a.concat(v), []).map(Key => ({ DeleteRequest: { Key } })),
        )

        Object.assign(json, other)

        return json
    }

    async then<TRes>(
        onfulfilled?: (value?: number) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        try {
            let i = 0
            for await (let m of this) {
                i++
            }
            onfulfilled(i)
        } catch (err) {
            onrejected(err)
        }
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }

    [Symbol.asyncIterator] = async function* (this: BatchWrite<M>) {
        for await (let res of Model[$batchWrite](this.toJSON())) {
            yield res
        }
    }
}

/* TYPES */

export interface BatchWriteOptions<M extends Model> extends Pick<DocumentClient.BatchWriteItemInput, 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'> {
    Model?: M['constructor']
    PutItems?: DocumentClient.PutItemInputAttributeMap[]
    DeleteKeys?: DocumentClient.KeyList
}
