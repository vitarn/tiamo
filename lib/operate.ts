import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model } from './model'
import { expression, ExpressionLogic } from './expression'

export class Operate<M extends Model> {
    private static paramSet = new Set([
        /** put */
        'TableName', 'Item',
        'ConditionExpression',
        'ExpressionAttributeNames', 'ExpressionAttributeValues',
        'ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics',
        /** get */
        'TableName', 'Key',
        'ProjectionExpression',
        'ExpressionAttributeNames',
        'ConsistentRead',
        'ReturnConsumedCapacity',
        /** query */
        'TableName', 'IndexName',
        'KeyConditionExpression', 'FilterExpression', 'ProjectionExpression',
        'ExpressionAttributeNames', 'ExpressionAttributeValues',
        'Select', 'Limit', 'ScanIndexForward', 'ExclusiveStartKey', 'ConsistentRead',
        'ReturnConsumedCapacity',
        /** scan */
        // legacy AttributesToGet ScanFilter ConditionalOperator
        'TableName', 'IndexName',
        'FilterExpression', 'ProjectionExpression',
        'ExpressionAttributeNames', 'ExpressionAttributeValues',
        'Select', 'Limit', 'ExclusiveStartKey', 'ConsistentRead',
        'TotalSegments', 'Segment',
        'ReturnConsumedCapacity',
        /** update */
        'TableName',
        'UpdateExpression', 'ConditionExpression',
        'ExpressionAttributeNames', 'ExpressionAttributeValues',
        'ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics',
        /** delete */
        'TableName',
        'ConditionExpression',
        'ExpressionAttributeNames', 'ExpressionAttributeValues',
        'ReturnValues', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics',
        /** batch get */
        // legacy AttributesToGet
        'GetKeys',
        'ProjectionExpression', 'ExpressionAttributeNames',
        'ConsistentRead',
        'ReturnConsumedCapacity',
        /** batch write */
        // legacy AttributesToGet
        'PutItems', 'DeleteKeys',
        'ReturnConsumedCapacity',
        'ReturnItemCollectionMetrics',
    ])

    constructor(options = {} as OperateOptions<M>) {
        this.options = { ...options }
    }

    protected options: OperateOptions<M>

    quiet() {
        this.options.ReturnConsumedCapacity = 'NONE'

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

    protected inspect() {
        return this.toJSON()
    }

    toJSON() {
        const { options } = this
        const { logic, leaf, keyExprs, filterExprs, setExprs, removeExprs, addExprs, deleteExprs, condExprs, projExprs, names, values } = options

        if (logic === 'NOT') {
            if (keyExprs && keyExprs.size) options.KeyConditionExpression = `NOT ${Array.from(keyExprs).join(' AND ')}`
            if (filterExprs && filterExprs.size) options.FilterExpression = `NOT ${Array.from(filterExprs).join(' AND ')}`
            if (condExprs && condExprs.size) options.ConditionExpression = `NOT ${Array.from(condExprs).join(' AND ')}`
        } else {
            if (keyExprs && keyExprs.size) options.KeyConditionExpression = `${Array.from(keyExprs).join(` ${logic} `)}`
            if (filterExprs && filterExprs.size) options.FilterExpression = `${Array.from(filterExprs).join(` ${logic} `)}`
            if (condExprs && condExprs.size) options.ConditionExpression = `${Array.from(condExprs).join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.KeyConditionExpression) options.KeyConditionExpression = `(${options.KeyConditionExpression})`
            if (options.FilterExpression) options.FilterExpression = `(${options.FilterExpression})`
            if (options.ConditionExpression) options.ConditionExpression = `(${options.ConditionExpression})`
        }

        if (setExprs || removeExprs || addExprs || deleteExprs) {
            const updateExprs = [
                setExprs && setExprs.size ? `SET ${Array.from(setExprs).join(', ')}` : '',
                removeExprs && removeExprs.size ? `REMOVE ${Array.from(removeExprs).join(', ')}` : '',
                addExprs && addExprs.size ? `ADD ${Array.from(addExprs).join(', ')}` : '',
                deleteExprs && deleteExprs.size ? `DELETE ${Array.from(deleteExprs).join(', ')}` : '',
            ].filter(e => e)
            if (updateExprs.length) options.UpdateExpression = updateExprs.join(' ')
        }

        if (projExprs && projExprs.size) options.ProjectionExpression = Array.from(projExprs).join(', ')
        if (names && Object.keys(names).length) options.ExpressionAttributeNames = names
        if (values && Object.keys(values).length) options.ExpressionAttributeValues = values

        const json = {}
        Operate.paramSet.forEach(key => {
            const value = options[key]
            if (typeof value !== 'undefined') {
                json[key] = value
            }
        })

        return json
    }

    async then<TRes>(
        onfulfilled?: (value?: any) => TRes | PromiseLike<TRes> | undefined | null | void,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        return onfulfilled()
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this['then'](null, onrejected)
    }
}

export class ReadOperate<M extends Model> extends Operate<M> {
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

    consistent(value = true) {
        if (value) {
            this.options.ConsistentRead = true
        } else {
            delete this.options.ConsistentRead
        }

        return this
    }
}

export class MultiReadOperate<M extends Model> extends ReadOperate<M> {
    protected static fork(logic: ExpressionLogic) {
        return new this({ logic, leaf: true })
    }

    filter<T extends this>(key: string) {
        const { options } = this
        const f = <V>(op: string, op2?: string) => (val?: V) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.filterExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as T
        }
        const compare = <V>(op2?) => ({
            eq: f<V>('=', op2),
            ne: f<V>('<>', op2),
            lt: f<V>('<', op2),
            lte: f<V>('<=', op2),
            gt: f<V>('>', op2),
            gte: f<V>('>=', op2),
        })

        return {
            ...compare<number | string>(),
            between: f<[number | string, number | string]>('BETWEEN'),
            in: f<(number | string)[]>('IN'),
            exists: f<never>('attribute_exists'),
            not: {
                exists: f<never>('attribute_not_exists'),
            },
            type: f<keyof DynamoDB.AttributeValue>('attribute_type'),
            begins: f<string>('begins_with'),
            contains: f<string>('contains'),
            size: compare<number>('size'),
        }
    }

    or(func: (operate: this) => any) {
        return this.logicalClause('OR', func)
    }

    not(func: (operate: this) => any) {
        return this.logicalClause('NOT', func)
    }

    private logicalClause<T extends typeof MultiReadOperate>(logic: ExpressionLogic, func: (operate) => any) {
        const operate = (this.constructor as any as T).fork(logic)
        func(operate)
        const json = operate.toJSON() as DocumentClient.QueryInput
        const { keyExprs, filterExprs, names, values } = this.options

        if (json.KeyConditionExpression) keyExprs.add(json.KeyConditionExpression)
        if (json.FilterExpression) filterExprs.add(json.FilterExpression)
        Object.assign(names, operate.options.names)
        Object.assign(values, operate.options.values)

        return this
    }

    limit(value: number) {
        if (!this.options.one && value > 0) this.options.Limit = value

        return this
    }
}

export class WriteOperate<M extends Model> extends Operate<M> {
    quiet() {
        super.quiet()

        this.options.ReturnItemCollectionMetrics = 'NONE'

        return this
    }
}

export class ConditionWriteOperate<M extends Model> extends WriteOperate<M> {
    where<T extends this>(key: string) {
        const { options } = this
        const f = <V>(op: string, op2?: string) => (val?: V) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.condExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as T
        }
        const compare = <V>(op2?) => ({
            eq: f<V>('=', op2),
            ne: f<V>('<>', op2),
            lt: f<V>('<', op2),
            lte: f<V>('<=', op2),
            gt: f<V>('>', op2),
            gte: f<V>('>=', op2),
        })

        return {
            ...compare<number | string>(),
            between: f<[string, string]>('BETWEEN'),
            in: f<string[]>('IN'),
            exists: f<never>('attribute_exists'),
            not: {
                exists: f<never>('attribute_not_exists'),
            },
            type: f<keyof DynamoDB.AttributeValue>('attribute_type'),
            begins: f<string>('begins_with'),
            contains: f<string>('contains'),
            size: compare<number>('size'),
        }
    }

    or(func: (operate: this) => any) {
        return this.logicalClause('OR', func)
    }

    not(func: (operate: this) => any) {
        return this.logicalClause('NOT', func)
    }

    private logicalClause<T extends typeof ConditionWriteOperate>(logic: ExpressionLogic, func: (operate) => any) {
        const operate = new (this.constructor as any as T)<M>({ logic, leaf: true })
        func(operate)
        const json = operate.toJSON() as DocumentClient.UpdateItemInput
        const { condExprs, names, values } = this.options

        if (json.ConditionExpression) condExprs.add(json.ConditionExpression)
        Object.assign(names, operate.options.names)
        Object.assign(values, operate.options.values)

        return this
    }

    quiet() {
        super.quiet()

        this.options.ReturnValues = 'NONE'

        return this
    }
}

/* TYPES */

export interface OperateOptions<M extends Model> extends
    Partial<DocumentClient.PutItemInput>,
    Partial<DocumentClient.QueryInput>,
    Partial<DocumentClient.UpdateItemInput> {
    Model?: M['constructor']
    logic?: ExpressionLogic
    leaf?: boolean
    one?: boolean

    /** KeyConditionExpression in query */
    keyExprs?: Set<string>
    /** KeyConditionExpression in query scan */
    filterExprs?: Set<string>
    /** UpdateExpression `SET` in update */
    setExprs?: Set<string>
    /** UpdateExpression `REMOVE` in update */
    removeExprs?: Set<string>
    /** UpdateExpression `ADD` in update */
    addExprs?: Set<string>
    /** UpdateExpression `DELETE` in update */
    deleteExprs?: Set<string>
    /** ConditionExpression in put update */
    condExprs?: Set<string>
    /** ProjectionExpression in get query scan batchGet */
    projExprs?: Set<string>
    /** ExpressionAttributeNames in all */
    names?: { [name: string]: string }
    /** ExpressionAttributeValues in query scan update put delete */
    values?: { [name: string]: any }

    /** RequestItems[TableName].Keys in batchGet */
    GetKeys?: DocumentClient.KeyList
    /** RequestItems[TableName][].PutRequest.Item in batchWrite */
    PutItems?: DocumentClient.PutItemInputAttributeMap[]
    /** RequestItems[TableName][].DeleteRequest.Key in batchWrite */
    DeleteKeys?: DocumentClient.KeyList
}
