import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $query } from './model'
import { expression, ExpressionLogic } from './expression'
import { MultiReadOperate, OperateOptions } from './operate'

export class Query<M extends Model, R extends M | M[]> extends MultiReadOperate<M> implements AsyncIterable<M> {
    constructor(protected options = {} as QueryOptions<M>) {
        super(options)

        this.options.logic = this.options.logic || 'AND'
        this.options.keyExprs = this.options.keyExprs || new Set()
        this.options.filterExprs = this.options.filterExprs || new Set()
        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}

        if (this.options.one) this.options.Limit = 1
    }

    where<T extends this>(key: string) {
        const { options } = this
        const f = <V>(op: string, op2?: string) => (val?: V) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.keyExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            // FIX: The inferred type of 'where' references an inaccessible 'this' type. A type annotation is necessary.
            return this as T
        }

        return {
            eq: f('='),
            lt: f('<'),
            lte: f('<='),
            gt: f('>'),
            gte: f('>='),
            between: f<[string, string]>('BETWEEN'),
            begins: f<string>('begins_with'),
        }
    }

    one() {
        this.options.one = true
        this.options.Limit = 1

        return this as Query<M, M>
    }

    /**
     * sort result
     * 
     * * forward: true | 1 | 'asc'
     * * backward: false | -1 | 'desc'
     * 
     * AWS param: ScanIndexForward = true
     */
    sort(order: boolean | 1 | -1 | 'asc' | 'desc' = true) {
        if (order === 'asc' || Number(order) > 0) {
            delete this.options.ScanIndexForward
        } else {
            this.options.ScanIndexForward = false
        }

        return this
    }

    inspect() {
        return { Query: super.inspect() }
    }

    toJSON() {
        return super.toJSON() as DocumentClient.QueryInput
    }

    /**
     * Count of results
     * 
     * Set `Select = 'COUNT'` then invoke dynamodb query operation.
     */
    async count() {
        this.options.Select = 'COUNT'

        const { Model } = this.options
        let total = 0
        for await (let items of Model[$query](this.toJSON())) {
            total += items as number
        }

        return total
    }

    async then<TRes>(
        onfulfilled: (value?: R) => TRes | PromiseLike<TRes> = (r => r) as any,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        try {
            let first
            let result = []
            for await (let res of this) {
                // Should not reach here if query result is empty
                if (this.options.one) {
                    first = res
                    break
                }
                result.push(res)
            }
            return onfulfilled(this.options.one ? first : result)
        } catch (err) {
            onrejected(err)
        }
    }

    [Symbol.asyncIterator] = async function* (this: Query<M, R>) {
        const { Model } = this.options

        for await (let items of Model[$query](this.toJSON())) {
            if (!Array.isArray(items) || !items.length) continue
            for (let props of items) {
                yield new Model(props) as M
            }
        }
    }
}

/* TYPES */

export interface QueryOptions<M extends Model> extends Pick<OperateOptions<M>, 'Model' | 'logic' | 'leaf' | 'keyExprs' | 'filterExprs' | 'projExprs' | 'names' | 'values'>, Partial<DocumentClient.QueryInput> {
    one?: boolean
}
