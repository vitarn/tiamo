import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $get, $batchGet } from './model'
import { expression } from './expression'

export class Get<M extends Model> {
    // legacy: AttributesToGet
    static PARAM_KEYS = `
        TableName Key
        ProjectionExpression ExpressionAttributeNames
        ConsistentRead
        ReturnConsumedCapacity
    `.split(/\s+/)

    constructor(private options = {} as GetOptions<M>) {
        options = { ...options }
        this.options = options

        options.projExprs = options.projExprs || new Set()
        options.names = options.names || {}
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
        return { Update: this.toJSON() }
    }

    toJSON(): Partial<DocumentClient.GetItemInput> & { Keys?: DocumentClient.KeyList } {
        const { options } = this
        const { projExprs, names } = options

        if (projExprs.size) options.ProjectionExpression = Array.from(projExprs).join(', ')
        if (Object.keys(names).length) options.ExpressionAttributeNames = names

        return ((this.constructor as any).PARAM_KEYS as string[]).reduce((json, key) => {
            const value = options[key]
            if (typeof value !== 'undefined') {
                json[key] = value
            }
            return json
        }, {})
    }

    async then<TRes>(
        onfulfilled?: (value?: M) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const { Model } = this.options

        return Model[$get](this.toJSON())
            .then(res => {
                if (res) return onfulfilled(new Model(res) as M)
                return onfulfilled()
            }, onrejected)
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }
}

/* TYPES */

export interface GetOptions<M extends Model> extends Partial<DocumentClient.GetItemInput> {
    Model?: M['constructor']
    projExprs?: Set<string>
    names?: { [name: string]: string }
}
