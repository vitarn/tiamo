import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model } from './model'
import { expression, ExpressionLogic } from './expression'

export class Update<M extends Model> {
    static UPDATE_KEYS = `
        Key TableName
        UpdateExpression ConditionExpression
        ExpressionAttributeNames ExpressionAttributeValues
        ReturnValues ReturnConsumedCapacity ReturnItemCollectionMetrics
    `.split(/\s+/)

    constructor(private options = {} as UpdateOptions<M>) {
        options = { ...options }
        this.options = options

        options.logic = options.logic || 'AND'
        options.setExprs = options.setExprs || []
        options.removeExprs = options.removeExprs || []
        options.addExprs = options.addExprs || []
        options.deleteExprs = options.deleteExprs || []
        options.condExprs = options.condExprs || []
        options.names = options.names || {}
        options.values = options.values || {}
    }

    where(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            options.condExprs = options.condExprs.concat(exprs)
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as Update<M>
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

    or(func: (update: this) => any) {
        return this.logicalClause('OR', func)
    }

    not(func: (update: this) => any) {
        return this.logicalClause('NOT', func)
    }

    private logicalClause(logic: ExpressionLogic, func: (update) => any) {
        const update = new Update<M>({ logic, leaf: true })
        func(update)
        const json = update.toJSON()
        const { condExprs, names, values } = this.options

        if (json.ConditionExpression) condExprs.push(json.ConditionExpression)
        Object.assign(names, update.options.names)
        Object.assign(values, update.options.values)

        return this
    }

    set(key: string) {
        const { options } = this
        const f = <T>(op: string, op2?: string) => (val?: T) => {
            const { exprs, names, values } = expression(key)(op, op2)(val)
            options.setExprs = options.setExprs.concat(exprs)
            Object.assign(options.names, names)
            Object.assign(options.values, values)
            return this as Update<M>
        }

        return {
            to: f('='),
            plus: f<number>('+'),
            minus: f<number>('-'),
            append: f('list_append'),
            prepend: f('list_append', 'prepend'),
            ifNotExists: f('if_not_exists'),
        }
    }

    remove(...keys: string[]) {
        const { options } = this
        keys.forEach(key => {
            const { exprs, names, values } = expression(key)('REMOVE')()
            options.removeExprs = options.removeExprs.concat(exprs)
            Object.assign(options.names, names)
            Object.assign(options.values, values)
        })

        return this
    }

    add(key: string, val: number | DocumentClient.DynamoDbSet | Set<number | string | DocumentClient.binaryType>) {
        const { options } = this
        const { exprs, names, values } = expression(key)('ADD')(val)
        options.addExprs = options.addExprs.concat(exprs)
        Object.assign(options.names, names)
        Object.assign(options.values, values)

        return this
    }

    delete(key: string, val: DocumentClient.DynamoDbSet | Set<number | string | DocumentClient.binaryType>) {
        const { options } = this
        const { exprs, names, values } = expression(key)('DELETE')(val)
        options.deleteExprs = options.deleteExprs.concat(exprs)
        Object.assign(options.names, names)
        Object.assign(options.values, values)

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
        const { logic, leaf, condExprs, setExprs, removeExprs, addExprs, deleteExprs, names, values } = options

        if (logic === 'NOT') {
            if (condExprs.length) options.ConditionExpression = `NOT ${condExprs.join(' AND ')}`
        } else {
            if (condExprs.length) options.ConditionExpression = `${condExprs.join(` ${logic} `)}`
        }

        if (leaf) {
            if (options.ConditionExpression) options.ConditionExpression = `(${options.ConditionExpression})`
        }

        const updateExprs = [
            setExprs.length ? `SET ${setExprs.join(', ')}` : '',
            removeExprs.length ? `REMOVE ${removeExprs.join(', ')}` : '',
            addExprs.length ? `ADD ${addExprs.join(', ')}` : '',
            deleteExprs.length ? `DELETE ${deleteExprs.join(', ')}` : '',
        ].filter(e => e)
        if (updateExprs.length) options.UpdateExpression = updateExprs.join(' ')

        if (Object.keys(names).length) options.ExpressionAttributeNames = names
        if (Object.keys(values).length) options.ExpressionAttributeValues = values

        return (this.constructor as typeof Update).UPDATE_KEYS.reduce((json, key) => {
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

        // Nothing to update
        if (!params.UpdateExpression) return onfulfilled()

        return this.options.Model._update(params)
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

export interface UpdateOptions<M extends Model> extends Partial<DocumentClient.UpdateItemInput> {
    Model?: M['constructor']
    logic?: ExpressionLogic
    leaf?: boolean
    setExprs?: string[]
    removeExprs?: string[]
    addExprs?: string[]
    deleteExprs?: string[]
    condExprs?: string[]
    names?: { [name: string]: string }
    values?: { [name: string]: any }
}
