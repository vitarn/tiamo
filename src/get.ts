import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $get } from './model'
import { expression } from './expression'
import { ReadOperate, OperateOptions } from './operate'

export class Get<M extends Model> extends ReadOperate<M> {
    // legacy: AttributesToGet
    static PARAM_KEYS = `
        TableName Key
        ProjectionExpression ExpressionAttributeNames
        ConsistentRead
        ReturnConsumedCapacity
    `.split(/\s+/)

    constructor(protected options = {} as GetOptions<M>) {
        super(options)

        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
    }

    inspect() {
        return { Get: super.inspect() }
    }

    toJSON() {
        return super.toJSON() as DocumentClient.GetItemInput
    }

    then<TRes>(
        onfulfilled: (value?: M) => TRes | PromiseLike<TRes> = (r => r) as any,
        onrejected?: (reason: any) => TRes | PromiseLike<TRes>,
    ) {
        const { Model } = this.options

        return Model[$get](this.toJSON())
            .then(res => {
                if (res) {
                    return onfulfilled(new Model(res) as M)
                }

                return onfulfilled()
            }, onrejected)
    }
}

/* TYPES */

export interface GetOptions<M extends Model> extends Pick<OperateOptions<M>, 'Model' | 'projExprs' | 'names'>, Partial<DocumentClient.GetItemInput> {
}
