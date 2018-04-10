import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $query } from './model'
import { expression, ExpressionLogic } from './expression'

export class Query<M extends Model, R extends M | M[]> implements AsyncIterable<M> {
    static PARAM_KEYS = `
        TableName IndexName
        KeyConditionExpression FilterExpression ProjectionExpression
        ExpressionAttributeNames ExpressionAttributeValues
        Select Limit ConsistentRead
        ScanIndexForward ExclusiveStartKey
        ReturnConsumedCapacity
    `.split(/\s+/)

    constructor(private options = {} as QueryOptions<M>) {
        this.options = options

        this.options.logic = this.options.logic || 'AND'
        this.options.keyExprs = this.options.keyExprs || new Set()
        this.options.filterExprs = this.options.filterExprs || new Set()
        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    where(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.keyExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            // FIX: The inferred type of 'where' references an inaccessible 'this' type. A type annotation is necessary.
            return this as Query<M, R>
        }

        return {
            eq: f('='),
            lt: f('<'),
            lte: f('<='),
            gt: f('>'),
            gte: f('>='),
            between: f<[any, any]>('BETWEEN'),
            begins: f<string>('begins_with'),
        }
    }

    filter(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.filterExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as Query<M, R>
        }
        const compare = (op2?) => ({
            eq: f('=', op2),
            ne: f('<>', op2),
            lt: f('<', op2),
            lte: f('<=', op2),
            gt: f('>', op2),
            gte: f('>=', op2),
        })

        return {
            ...compare(),
            between: f<[any, any]>('BETWEEN'),
            in: f<any[]>('IN'),
            exists: f<never>('attribute_exists'),
            not: {
                exists: f<never>('attribute_not_exists'),
            },
            type: f<keyof DynamoDB.AttributeValue>('attribute_type'),
            begins: f<string>('begins_with'),
            contains: f('contains'),
            size: compare('size'),
        }
    }

    or(func: (query: this) => any) {
        return this.logicalClause('OR', func)
    }

    not(func: (query: this) => any) {
        return this.logicalClause('NOT', func)
    }

    private logicalClause(logic: ExpressionLogic, func: (query) => any) {
        const query = new Query({ logic, leaf: true })
        func(query)
        const json = query.toJSON()
        const { keyExprs, filterExprs, names, values } = this.options

        if (json.KeyConditionExpression) keyExprs.add(json.KeyConditionExpression)
        if (json.FilterExpression) filterExprs.add(json.FilterExpression)
        Object.assign(names, query.options.names)
        Object.assign(values, query.options.values)

        return this
    }

    /**
     * Build ProjectionExpression
     */
    select(...keys: (string | string[])[]) {
        const { options } = this

        keys.reduce<string[]>((a, v) => typeof v === 'string' ? a.concat(v) : a.concat(...v), [])
            .filter(key => typeof key === 'string')
            .map(key => key.split(/\s+/))
            .reduce((a, v) => a.concat(v), [])
            .forEach(key => {
                const { exprs, names } = expression(key)()()
                exprs.forEach(e => options.projExprs.add(e))
                Object.assign(options.names, names)
            })

        return this
    }

    limit(val: number) {
        if (!this.options.one && val > 0) this.options.Limit = val

        return this
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

    consistent(value = true) {
        if (!!value === true) {
            this.options.ConsistentRead = true
        } else {
            delete this.options.ConsistentRead
        }

        return this
    }

    index(name?: string) {
        if (typeof name === 'string') {
            this.options.IndexName = name
        } else {
            delete this.options.IndexName
        }

        return this
    }

    inspect() {
        return { Query: this.toJSON() }
    }

    toJSON(): Partial<DocumentClient.QueryInput> {
        const { options } = this
        const { logic, leaf, keyExprs, filterExprs, projExprs, names, values } = options

        if (logic === 'NOT') {
            if (keyExprs.size) options.KeyConditionExpression = `NOT ${Array.from(keyExprs).join(' AND ')}`
            if (filterExprs.size) options.FilterExpression = `NOT ${Array.from(filterExprs).join(' AND ')}`
        } else {
            if (keyExprs.size) options.KeyConditionExpression = `${Array.from(keyExprs).join(` ${logic} `)}`
            if (filterExprs.size) options.FilterExpression = `${Array.from(filterExprs).join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.KeyConditionExpression) options.KeyConditionExpression = `(${options.KeyConditionExpression})`
            if (options.FilterExpression) options.FilterExpression = `(${options.FilterExpression})`
        }

        if (projExprs.size) options.ProjectionExpression = Array.from(projExprs).join(', ')

        if (Object.keys(names).length) options.ExpressionAttributeNames = names
        if (Object.keys(values).length) options.ExpressionAttributeValues = values

        if (options.one && !options.Limit) options.Limit = 1

        return (this.constructor as typeof Query).PARAM_KEYS.reduce((json, key) => {
            const value = options[key]
            if (typeof value !== 'undefined') {
                json[key] = value
            }
            return json
        }, {})
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
        onfulfilled?: (value?: R) => TRes | PromiseLike<TRes>,
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
            onfulfilled(this.options.one ? first : result)
        } catch (err) {
            onrejected(err)
        }
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
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

export interface QueryOptions<M extends Model> extends Partial<DocumentClient.QueryInput> {
    Model?: M['constructor']
    one?: boolean
    logic?: ExpressionLogic
    leaf?: boolean
    keyExprs?: Set<string>
    filterExprs?: Set<string>
    projExprs?: Set<string>
    names?: { [name: string]: string }
    values?: { [name: string]: any }
}
