import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model } from './model'
import { expression, ExpressionLogic } from './expression'

export class Query<M extends Model, R extends M | M[]> {
    static QUERY_KEYS = `
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
        this.options.keyExprs = this.options.keyExprs || []
        this.options.filterExprs = this.options.filterExprs || []
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    where(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            options.keyExprs = options.keyExprs.concat(exprs)
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
            options.filterExprs = options.filterExprs.concat(exprs)
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

        if (json.KeyConditionExpression) keyExprs.push(json.KeyConditionExpression)
        if (json.FilterExpression) filterExprs.push(json.FilterExpression)
        Object.assign(names, query.options.names)
        Object.assign(values, query.options.values)

        return this
    }

    inspect() {
        return { Query: this.toJSON() }
    }

    toJSON(): Partial<DocumentClient.QueryInput> {
        const { options } = this
        const { logic, leaf, keyExprs, filterExprs, names, values } = options

        if (logic === 'NOT') {
            if (keyExprs.length) options.KeyConditionExpression = `NOT ${keyExprs.join(' AND ')}`
            if (filterExprs.length) options.FilterExpression = `NOT ${filterExprs.join(' AND ')}`
        } else {
            if (keyExprs.length) options.KeyConditionExpression = `${keyExprs.join(` ${logic} `)}`
            if (filterExprs.length) options.FilterExpression = `${filterExprs.join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.KeyConditionExpression) options.KeyConditionExpression = `(${options.KeyConditionExpression})`
            if (options.FilterExpression) options.FilterExpression = `(${options.FilterExpression})`
        }

        if (Object.keys(names).length) options.ExpressionAttributeNames = names
        if (Object.keys(values).length) options.ExpressionAttributeValues = values

        return (this.constructor as typeof Query).QUERY_KEYS.reduce((json, key) => {
            if (options[key]) {
                json[key] = options[key]
            }
            return json
        }, {})
    }

    get count() {
        this.options.Select = 'COUNT'
        return this
    }

    limit(val: number) {
        this.options.Limit = val
        return this
    }

    // get asc() {
    //     this.options.ScanIndexForward = true
    //     return this
    // }

    get desc() {
        this.options.ScanIndexForward = false
        return this
    }

    // sort(val) {
    //     if (val === 1 || val === true) {
    //         this.options.ScanIndexForward = true
    //     } else if (val === -1 || val === false) {
    //         this.options.ScanIndexForward = false
    //     }
    // }

    get consistent() {
        this.options.ConsistentRead = true
        return this
    }

    index(name: string) {
        this.options.IndexName = name
        return this
    }

    then<TRes>(
        onfulfilled?: (value: R) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const params = this.toJSON()

        return this.options.Model._query(params)
            .then(res => {
                if (this.options.one) {
                    if (res.length === 0) {
                        onfulfilled(null)
                    } else {
                        onfulfilled(new this.options.Model(res[0]) as any)
                    }
                } else {
                    onfulfilled(res.map(attr => new this.options.Model(attr)) as any)
                }
            })
            .catch(onrejected)
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }
}

/* TYPES */

export interface QueryOptions<M extends Model> extends Partial<DocumentClient.QueryInput> {
    Model?: M['constructor']
    one?: boolean
    logic?: ExpressionLogic
    leaf?: boolean
    keyExprs?: string[]
    filterExprs?: string[]
    setExprs?: string[]
    removeExprs?: string[]
    deleteExprs?: string[]
    names?: { [name: string]: string }
    values?: { [name: string]: any }
}
