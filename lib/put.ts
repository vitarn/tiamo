import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $put } from './model'
import { expression, ExpressionLogic } from './expression'
import { ConditionWriteOperate, OperateOptions } from './operate'

export class Put<M extends Model> extends ConditionWriteOperate<M> {
    constructor(protected options = {} as PutOptions<M>) {
        super(options)

        this.options.logic = this.options.logic || 'AND'
        this.options.condExprs = this.options.condExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    inspect() {
        return { Delete: super.inspect() }
    }

    toJSON(): Partial<DocumentClient.PutItemInput> {
        return super.toJSON()
    }

    then<TRes>(
        onfulfilled?: (value?: M) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const params = this.toJSON()

        // Nothing to put
        if (!params.Item) {
            onrejected(new Error('Put item is empty'))
            return
        }

        return this.options.Model[$put](params)
            .then(res => {
                // res is old value or undefined. so we merge Item into res.
                onfulfilled(new this.options.Model(Object.assign(res || {}, params.Item)) as M)
            }, onrejected)
    }
}

/* TYPES */

export interface PutOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'logic' | 'leaf' | 'condExprs' | 'names' | 'values'>,
    Partial<DocumentClient.PutItemInput> {
}
