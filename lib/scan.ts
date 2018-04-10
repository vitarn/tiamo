import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $scan } from './model'
import { expression, ExpressionLogic } from './expression'

export class Scan<M extends Model> implements AsyncIterable<M>{
    // legacy AttributesToGet ScanFilter ConditionalOperator
    static PARAM_KEYS = `
        TableName IndexName
        FilterExpression ProjectionExpression
        ExpressionAttributeNames ExpressionAttributeValues
        Select Limit ConsistentRead
        ExclusiveStartKey
        TotalSegments Segment
        ReturnConsumedCapacity
    `.split(/\s+/)

    constructor(private options = {} as ScanOptions<M>) {
        this.options = options

        this.options.logic = this.options.logic || 'AND'
        this.options.filterExprs = this.options.filterExprs || new Set()
        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    /**
     * Build FilterExpression
     */
    filter(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.filterExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as Scan<M>
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

    or(func: (scan: this) => any) {
        return this.logicalClause('OR', func)
    }

    not(func: (scan: this) => any) {
        return this.logicalClause('NOT', func)
    }

    private logicalClause(logic: ExpressionLogic, func: (scan) => any) {
        const scan = new Scan({ logic, leaf: true })
        func(scan)
        const json = scan.toJSON()
        const { filterExprs, projExprs, names, values } = this.options

        if (json.FilterExpression) filterExprs.add(json.FilterExpression)
        if (json.ProjectionExpression) projExprs.add(json.ProjectionExpression)
        Object.assign(names, scan.options.names)
        Object.assign(values, scan.options.values)

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
                options.projExprs.add(exprs[0])
                Object.assign(options.names, names)
            })

        return this
    }

    inspect() {
        return { Scan: this.toJSON() }
    }

    toJSON(): Partial<DocumentClient.ScanInput> {
        const { options } = this
        const { logic, leaf, filterExprs, projExprs, names, values } = options

        if (logic === 'NOT') {
            if (filterExprs.size) options.FilterExpression = `NOT ${Array.from(filterExprs).join(' AND ')}`
        } else {
            if (filterExprs.size) options.FilterExpression = `${Array.from(filterExprs).join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.FilterExpression) options.FilterExpression = `(${options.FilterExpression})`
        }

        if (projExprs.size) options.ProjectionExpression = Array.from(projExprs).join(', ')

        if (Object.keys(names).length) options.ExpressionAttributeNames = names
        if (Object.keys(values).length) options.ExpressionAttributeValues = values

        return (this.constructor as typeof Scan).PARAM_KEYS.reduce((json, key) => {
            const value = options[key]
            if (typeof value !== 'undefined') {
                json[key] = value
            }
            return json
        }, {})
    }

    limit(val: number) {
        if (val > 0) this.options.Limit = val

        return this
    }

    consistent() {
        this.options.ConsistentRead = true

        return this
    }

    index(name: string) {
        this.options.IndexName = name

        return this
    }

    /**
     * Count of results
     * 
     * Set `Select = 'COUNT'` then invoke dynamodb scan operation.
     */
    async count() {
        this.options.Select = 'COUNT'

        const { Model } = this.options
        let total = 0
        for await (let items of Model[$scan](this.toJSON())) {
            total += items as number
        }

        return total
    }

    async then<TRes>(
        onfulfilled?: (value?: M[]) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        try {
            let result = []
            for await (let res of this) {
                result.push(res)
            }
            onfulfilled(result)
        } catch (err) {
            onrejected(err)
        }
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }

    [Symbol.asyncIterator] = async function* (this: Scan<M>) {
        const { Model } = this.options

        for await (let items of Model[$scan](this.toJSON())) {
            if (!Array.isArray(items) || !items.length) continue
            for (let props of items) {
                yield new Model(props) as M
            }
        }
    }
}

/* TYPES */

export interface ScanOptions<M extends Model> extends Partial<DocumentClient.ScanInput> {
    Model?: M['constructor']
    logic?: ExpressionLogic
    leaf?: boolean
    filterExprs?: Set<string>
    projExprs?: Set<string>
    names?: { [name: string]: string }
    values?: { [name: string]: any }
}
