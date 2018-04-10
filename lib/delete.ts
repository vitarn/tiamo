import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $delete } from './model'
import { expression, ExpressionLogic } from './expression'
import { ConditionWriteOperate, OperateOptions } from './operate'

export class Delete<M extends Model> extends ConditionWriteOperate<M> {
    constructor(protected options = {} as DeleteOptions<M>) {
        super(options)

        this.options.logic = this.options.logic || 'AND'
        this.options.condExprs = this.options.condExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    inspect() {
        return { Delete: super.inspect() }
    }

    toJSON(): Partial<DocumentClient.UpdateItemInput> {
        return super.toJSON()
    }

    then<TRes>(
        onfulfilled?: (value?: M) => TRes | PromiseLike<TRes>,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const params = this.toJSON()

        // Nothing to delete
        if (!params.Key) {
            onrejected(new Error('Delete key is empty'))
            return
        }

        return this.options.Model[$delete](params)
            .then(res => {
                if (res) return onfulfilled(new this.options.Model(res) as M)
                return onfulfilled()
            }, onrejected)
    }
}

/* TYPES */

export interface DeleteOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'logic' | 'leaf' | 'condExprs' | 'names' | 'values'>,
    Partial<DocumentClient.DeleteItemInput> {
}
