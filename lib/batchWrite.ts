import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $batchWrite } from './model'
import { expression } from './expression'
import { BatchGet } from './batchGet'
import { WriteOperate, OperateOptions } from './operate'

export class BatchWrite<M extends Model> extends WriteOperate<M> implements AsyncIterable<M> {
    // legacy: AttributesToGet
    // static PARAM_KEYS = `
    //     PutItems DeleteKeys
    //     ReturnConsumedCapacity
    //     ReturnItemCollectionMetrics
    // `.split(/\s+/)

    constructor(protected options = {} as BatchWriteOptions<M>) {
        super(options)

        this.options.PutItems = this.options.PutItems || []
        this.options.DeleteKeys = this.options.DeleteKeys || []
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

    inspect() {
        return { BatchWrite: this.toJSON() }
    }

    toJSON() {
        const { PutItems = [], DeleteKeys = [], ...other } = super.toJSON() as Pick<BatchWriteOptions<M>, 'PutItems' | 'DeleteKeys' | 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'>
        const { options } = this
        const { Model } = options
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

    [Symbol.asyncIterator] = async function* (this: BatchWrite<M>) {
        for await (let res of Model[$batchWrite](this.toJSON())) {
            yield res
        }
    }
}

/* TYPES */

export interface BatchWriteOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'PutItems' | 'DeleteKeys'>,
    Pick<DocumentClient.BatchWriteItemInput, 'ReturnConsumedCapacity' | 'ReturnItemCollectionMetrics'> {
}
