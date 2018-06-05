import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $update } from './model'
import { expression } from './expression'
import { ConditionWriteOperate, OperateOptions } from './operate'

export class Update<M extends Model> extends ConditionWriteOperate<M> {
    // static UPDATE_KEYS = `
    //     Key TableName
    //     UpdateExpression ConditionExpression
    //     ExpressionAttributeNames ExpressionAttributeValues
    //     ReturnValues ReturnConsumedCapacity ReturnItemCollectionMetrics
    // `.split(/\s+/)

    constructor(protected options = {} as UpdateOptions<M>) {
        super(options)

        this.options.logic = this.options.logic || 'AND'
        this.options.setExprs = this.options.setExprs || new Set()
        this.options.removeExprs = this.options.removeExprs || new Set()
        this.options.addExprs = this.options.addExprs || new Set()
        this.options.deleteExprs = this.options.deleteExprs || new Set()
        this.options.condExprs = this.options.condExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    set(key: string) {
        const { options } = this
        const f = <V>(op: string, op2?: string) => (val?: V) => {
            if (op === '=' && (val as any) === '') {
                // return this.remove(key)
                val = null
            }

            const { exprs, names, values } = expression(key)(op, op2)(val)
            exprs.forEach(e => options.setExprs.add(e))
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
            exprs.forEach(e => options.removeExprs.add(e))
            Object.assign(options.names, names)
            Object.assign(options.values, values)
        })

        return this
    }

    add(key: string, val: number | DocumentClient.DynamoDbSet | Set<number | string | DocumentClient.binaryType>) {
        const { options } = this
        const { exprs, names, values } = expression(key)('ADD')(val)
        exprs.forEach(e => options.addExprs.add(e))
        Object.assign(options.names, names)
        Object.assign(options.values, values)

        return this
    }

    delete(key: string, val: DocumentClient.DynamoDbSet | Set<number | string | DocumentClient.binaryType>) {
        const { options } = this
        const { exprs, names, values } = expression(key)('DELETE')(val)
        exprs.forEach(e => options.deleteExprs.add(e))
        Object.assign(options.names, names)
        Object.assign(options.values, values)

        return this
    }

    inspect() {
        return { Update: super.inspect() }
    }

    toJSON(): Partial<DocumentClient.UpdateItemInput> {
        return super.toJSON()
    }

    then<TRes>(
        onfulfilled: (value?: M) => TRes | PromiseLike<TRes> = (r => r) as any,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const params = this.toJSON()

        // Nothing to update
        if (!params.UpdateExpression) {
            onrejected(new Error('Update expression is empty'))
            return
        }

        return this.options.Model[$update](params)
            .then(res => {
                if (res) return onfulfilled(new this.options.Model(res) as M)
                return onfulfilled()
            }, onrejected)
    }
}

/* TYPES */

export interface UpdateOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'logic' | 'leaf' | 'condExprs' | 'setExprs' | 'removeExprs' | 'addExprs' | 'deleteExprs' | 'names' | 'values'>,
    Partial<DocumentClient.UpdateItemInput> {
}
