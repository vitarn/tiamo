import { DynamoDB } from 'aws-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Model, $scan } from './model'
import { expression, ExpressionLogic } from './expression'
import { MultiReadOperate, OperateOptions } from './operate'

export class Scan<M extends Model> extends MultiReadOperate<M> implements AsyncIterable<M>{
    constructor(protected options = {} as ScanOptions<M>) {
        super(options)

        this.options.logic = this.options.logic || 'AND'
        this.options.filterExprs = this.options.filterExprs || new Set()
        this.options.projExprs = this.options.projExprs || new Set()
        this.options.names = this.options.names || {}
        this.options.values = this.options.values || {}
    }

    inspect() {
        return { Scan: super.inspect() }
    }

    toJSON(): Partial<DocumentClient.ScanInput> {
        return super.toJSON()
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

export interface ScanOptions<M extends Model> extends
    Pick<OperateOptions<M>, 'Model' | 'logic' | 'leaf' | 'filterExprs' | 'projExprs' | 'names' | 'values'>,
    Partial<DocumentClient.ScanInput> {
    // logic?: ExpressionLogic
    // leaf?: boolean
    // filterExprs?: Set<string>
    // projExprs?: Set<string>
    // names?: { [name: string]: string }
    // values?: { [name: string]: any }
}
