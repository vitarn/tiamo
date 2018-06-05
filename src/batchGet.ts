import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $batchGet } from './model'
import { expression } from './expression'
import { BatchWrite } from './batchWrite'
import { ReadOperate, OperateOptions } from './operate'

export class BatchGet<M extends Model> extends ReadOperate<M> implements AsyncIterable<M> {
    constructor(protected options = {} as BatchGetOptions<M>) {
        super(options)

        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
    }

    /**
     * Append more Keys
     */
    get(...GetKeys: DocumentClient.KeyList) {
        this.options.GetKeys.push(...GetKeys)

        return this
    }

    inspect() {
        return { BatchGet: this.toJSON() }
    }

    toJSON() {
        const { GetKeys = [], ReturnConsumedCapacity, ...other } = super.toJSON() as Pick<BatchGetOptions<M>, 'GetKeys' | 'ReturnConsumedCapacity' | 'projExprs' | 'names' | 'ConsistentRead'>
        const { options } = this
        const { Model } = options
        const { tableName } = Model
        const json = {
            RequestItems: {
                [tableName]: Object.assign(other, {
                    /**
                     * Array.flatten shim by reduce
                     *
                     * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/flatten
                     */
                    Keys: GetKeys.reduce((a, v) => a.concat(v), []),
                }),
            },
        } as DocumentClient.BatchGetItemInput

        if (options.ReturnConsumedCapacity) json.ReturnConsumedCapacity = options.ReturnConsumedCapacity

        return json
    }

    async then<TRes>(
        onfulfilled: (value?: M[]) => TRes | PromiseLike<TRes> = (r => r) as any,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        try {
            let result = [] as M[]
            for await (let m of this) {
                result.push(m)
            }
            return onfulfilled(result)
        } catch (err) {
            if (onrejected) {
                onrejected(err)
            } else {
                throw err
            }
        }
    }

    [Symbol.asyncIterator] = async function* (this: BatchGet<M>) {
        const { Model, batchWrite } = this.options

        if (batchWrite) for await (let res of batchWrite) { }

        const params = this.toJSON()
        const items = [...params.RequestItems[Model.tableName].Keys]

        do {
            // batchGet max 100
            const part = params.RequestItems[Model.tableName].Keys = items.splice(0, 100)

            for await (let map of Model[$batchGet](params)) {
                for (let props of map[Model.tableName]) {
                    yield new Model(props) as M
                }
            }
        } while (items.length)
    }
}

/* TYPES */

export interface BatchGetOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'GetKeys' | 'projExprs' | 'names'>,
    Pick<DocumentClient.BatchGetItemInput, 'ReturnConsumedCapacity'>,
    Pick<DocumentClient.KeysAndAttributes, 'ConsistentRead' | 'ProjectionExpression' | 'ExpressionAttributeNames'> {
    batchWrite?: BatchWrite<M>
}
