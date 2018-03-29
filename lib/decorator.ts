import debug from './debug'
import { metadataFor, createDecorator, required, optional, HandleDescriptor, FlexibleDecorator} from 'tdv'
import { dynamodbFor } from './metadata'
import { Model } from './model'

const log = debug('decorator')

export { required, optional } from 'tdv'

const tableDescriptor: HandleDescriptor = (target, [name]) => {
    log('tableDescriptor', target, name)
    const dynamodb = dynamodbFor((target as Function).prototype)

    if (typeof name === 'string') {
        dynamodb.tableName = name
    } else if (typeof target === 'function') {
        dynamodb.tableName = target.name
    }
}

/**
 * Set DynamoDB table name for `Model` class
 * 
 *      `@tableName class Foo extends Model {} // Foo.tableName === 'Foo'`
 *      `@tableName('foos') class Foo extends Model {} // Foo.tableName === 'foos'`
 */
export const tableName: FlexibleDecorator<string> = (...args) => {
    return createDecorator(tableDescriptor, args)
}

const keyDescriptor: (type: 'hash' | 'range') => HandleDescriptor = type => (target, key, desc) => {
    log('keyDescriptor', target, key, desc)

    const metadata = metadataFor(target, key)

    // Dont rewrite existed tdv metadata
    if (!metadata['tdv:joi'] && !metadata['tdv:ref']) {
        required(target, key, desc)
    }

    metadata[`tiamo:${type}`] = true
    dynamodbFor(target)[`${type}Key`] = key
}

/**
 * Mark DynamoDB hash key
 */
export const hashKey: FlexibleDecorator<never> = (...args) => {
    return createDecorator(keyDescriptor('hash'), args)
}

/**
 * Mark DynamoDB range key
 */
export const rangeKey: FlexibleDecorator<never> = (...args) => {
    return createDecorator(keyDescriptor('range'), args)
}

const indexDescriptor: (global?: boolean) => HandleDescriptor = global => (target, key, desc, [options]) => {
    const scope = global ? 'global' : 'local'
    const defaultType = global ? 'hash' : 'range'
    const opts: IndexKeyOptions = options || {}

    opts.name = opts.name || `${key}-${scope}`
    opts.type = opts.type || defaultType

    log('indexDescriptor', target, key, desc, opts)

    const metadata = metadataFor(target, key)

    // Dont rewrite existed tdv metadata
    if (!metadata['tdv:joi'] && !metadata['tdv:ref']) {
        optional(target, key, desc)
    }

    metadata[`tiamo:index:${scope}`] = opts

    const dynamodb = dynamodbFor(target)
    const indexes: any[] = dynamodb[`${scope}Indexes`] = dynamodb[`${scope}Indexes`] || []
    let index = indexes.find(i => i.name === opts.name)
    if (index) {
        index[opts.type] = key
    } else {
        index = {
            name: opts.name,
            [opts.type]: key,
        }
        indexes.push(index)
    }
}

/**
 * Mark DynamoDB global index
 */
export const globalIndex: FlexibleDecorator<IndexKeyOptions> = (...args) => {
    return createDecorator(indexDescriptor(true), args)
}

/**
 * Mark DynamoDB local index
 */
export const localIndex: FlexibleDecorator<IndexKeyOptions> = (...args) => {
    return createDecorator(indexDescriptor(), args)
}

export type IndexKeyOptions = {
    name?: string
    type?: 'hash' | 'range'
}
