import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model } from './model'
import { expression, ExpressionLogic } from './expression'

export class Operate<M extends Model> {
    private static paramSet = new Set([
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
        'GetKeys',
        'ProjectionExpression', 'ExpressionAttributeNames',
        'ConsistentRead',
        'ReturnConsumedCapacity',
        /** batch write */
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

    inspect() {
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
        onfulfilled?: (value?: M) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        return onfulfilled()
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this['then'](null, onrejected)
    }
}

export class ReadOperate<M extends Model> extends Operate<M> {
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

export class WriteOperate<M extends Model> extends Operate<M> {
}

/* TYPES */

export interface OperateOptions<M extends Model> extends Partial<DocumentClient.QueryInput>, Partial<DocumentClient.UpdateItemInput> {
    Model?: M['constructor']
    logic?: ExpressionLogic
    leaf?: boolean

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
}
