import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $batchGet } from './model'
import { expression } from './expression'
import { BatchWrite } from './batchWrite'

export class BatchGet<M extends Model> implements AsyncIterable<M> {
    // legacy: AttributesToGet
    static PARAM_KEYS = `
        GetKeys
        ProjectionExpression ExpressionAttributeNames
        ConsistentRead
        ReturnConsumedCapacity
    `.split(/\s+/)

    constructor(private options = {} as BatchGetOptions<M>) {
        options = { ...options }
        this.options = options

        options.projExprs = options.projExprs || new Set()
        options.names = options.names || {}
    }

    get(...GetKeys: DocumentClient.KeyList) {
        this.options.GetKeys.push(...GetKeys)

        return this
    }

    select(...keys: (string | string[])[]) {
        const { options } = this

        keys.reduce<string[]>((a, v) => typeof v === 'string' ? a.concat(v) : a.concat(...v), [])
            .filter(key => typeof key === 'string')
            .map(key => key.split(/\s+/))
            .reduce((a, v) => a.concat(v), [])
            .forEach(key => {
                const { exprs, names } = expression(key)()()
                options.projExprs.add(exprs[0])
                Object.assign(options.names, names)
            })

        return this
    }

    consistent(value = true) {
        if (value) {
            this.options.ConsistentRead = true
        } else {
            delete this.options.ConsistentRead
        }

        return this
    }

    quiet() {
        this.options.ReturnConsumedCapacity = 'NONE'

        return this
    }

    inspect() {
        return { BatchGet: this.toJSON() }
    }

    toJSON() {
        const { options } = this
        const { Model, GetKeys = [], projExprs, names } = options
        const { tableName } = Model

        if (projExprs.size) options.ProjectionExpression = Array.from(projExprs).join(', ')
        if (Object.keys(names).length) options.ExpressionAttributeNames = names

        const json = {
            RequestItems: {
                [tableName]: {
                    /**
                     * Array.flatten shim by reduce
                     *
                     * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/flatten
                     */
                    Keys: GetKeys.reduce((a, v) => a.concat(v), []),
                },
            },
        } as DocumentClient.BatchGetItemInput
        const keysAndAttributes = json.RequestItems[tableName]

        if (options.ProjectionExpression) keysAndAttributes.ProjectionExpression = options.ProjectionExpression
        if (options.ExpressionAttributeNames) keysAndAttributes.ExpressionAttributeNames = options.ExpressionAttributeNames
        if (options.ConsistentRead) keysAndAttributes.ConsistentRead = options.ConsistentRead
        if (options.ReturnConsumedCapacity) json.ReturnConsumedCapacity = options.ReturnConsumedCapacity

        return json
    }

    async then<TRes>(
        onfulfilled?: (value?: M[]) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        try {
            let result = [] as M[]
            for await (let m of this) {
                result.push(m)
            }
            onfulfilled(result)
        } catch (err) {
            onrejected(err)
        }
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }

    [Symbol.asyncIterator] = async function* (this: BatchGet<M>) {
        const { Model, batchWrite } = this.options

        if (batchWrite) for await (let res of batchWrite) { }

        for await (let map of Model[$batchGet](this.toJSON())) {
            for (let props of map[Model.tableName]) {
                yield new Model(props) as M
            }
        }
    }
}

/* TYPES */

export interface BatchGetOptions<M extends Model> extends
    Pick<DocumentClient.BatchGetItemInput, 'ReturnConsumedCapacity'>,
    Pick<DocumentClient.KeysAndAttributes, 'ConsistentRead' | 'ProjectionExpression' | 'ExpressionAttributeNames'> {
    Model?: M['constructor']
    GetKeys?: DocumentClient.KeyList
    batchWrite?: BatchWrite<M>
    projExprs?: Set<string>
    names?: { [name: string]: string }
}
