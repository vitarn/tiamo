import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model } from './model'
import { expression, ExpressionLogic } from './expression'

export class Delete<M extends Model> {
    static PARAM_KEYS = `
        Key TableName
        ConditionExpression
        ExpressionAttributeNames ExpressionAttributeValues
        ReturnValues ReturnConsumedCapacity ReturnItemCollectionMetrics
    `.split(/\s+/)

    constructor(private options = {} as DeleteOptions<M>) {
        options = { ...options }
        this.options = options

        options.logic = options.logic || 'AND'
        options.condExprs = options.condExprs || []
        options.names = options.names || {}
        options.values = options.values || {}
    }

    where(key: string) {
        const { options } = this
        const f = this._whereClause(key)
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
            contains: f<string>('contains'),
            size: compare('size'),
        }
    }

    protected _whereClause = (key: string) => <T>(op: string, op2?: string) => (val?: T) => {
        const { options } = this
        const { exprs, names, values } = expression(key)(op, op2)(val)

        options.condExprs = options.condExprs.concat(exprs)
        Object.assign(options.names, names)
        Object.assign(options.values, values)

        return this as Delete<M>
    }

    or(func: (child: this) => any) {
        return this._logicalClause('OR', func)
    }

    not(func: (child: this) => any) {
        return this._logicalClause('NOT', func)
    }

    private _logicalClause(logic: ExpressionLogic, func: (child) => any) {
        const instance = new Delete({ logic, leaf: true })
        func(instance)
        const json = instance.toJSON()
        const { condExprs, names, values } = this.options

        if (json.ConditionExpression) condExprs.push(json.ConditionExpression)
        Object.assign(names, instance.options.names)
        Object.assign(values, instance.options.values)

        return this
    }

    quiet() {
        this.options.ReturnValues = 'NONE'
        this.options.ReturnConsumedCapacity = 'NONE'
        this.options.ReturnItemCollectionMetrics = 'NONE'

        return this
    }

    inspect() {
        return { Update: this.toJSON() }
    }

    toJSON(): Partial<DocumentClient.UpdateItemInput> {
        const { options } = this
        const { logic, leaf, condExprs, names, values } = options

        if (logic === 'NOT') {
            if (condExprs.length) options.ConditionExpression = `NOT ${condExprs.join(' AND ')}`
        } else {
            if (condExprs.length) options.ConditionExpression = `${condExprs.join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.ConditionExpression) options.ConditionExpression = `(${options.ConditionExpression})`
        }

        if (Object.keys(names).length) options.ExpressionAttributeNames = names
        if (Object.keys(values).length) options.ExpressionAttributeValues = values

        return ((this.constructor as any).PARAM_KEYS as string[]).reduce((json, key) => {
            if (options[key]) {
                json[key] = options[key]
            }
            return json
        }, {})
    }

    then<TRes>(
        onfulfilled?: (value?: M) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const params = this.toJSON()

        return this.options.Model._delete(params)
            .then(res => {
                if (res) onfulfilled(new this.options.Model(res) as M)
                return onfulfilled(null)
            }, onrejected)
    }

    catch(onrejected?: (reason: any) => PromiseLike<never>) {
        return this.then(null, onrejected)
    }
}

/* TYPES */

export interface DeleteOptions<M extends Model> extends Partial<DocumentClient.DeleteItemInput> {
    Model?: M['constructor']
    logic?: ExpressionLogic
    leaf?: boolean
    condExprs?: string[]
    names?: { [name: string]: string }
    values?: { [name: string]: any }
}
