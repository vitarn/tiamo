import { metadataFor, createDecorater, required, HandleDescriptor, FlexibleDecorator} from 'tdv'
import { dynamodbFor } from './metadata'
import { Model } from './model'
import { log } from './log'

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
    return createDecorater(tableDescriptor, args)
}

const keyDescriptor: (type: 'hash' | 'range') => HandleDescriptor = type => (target, key, desc) => {
    log('keyDescriptor', target, key, desc)
    required(target, key, desc)
    metadataFor(target, key)[`tdmo:${type}`] = true
}

/**
 * Mark DynamoDB hash key
 */
export const hashKey = (...args) => {
    return createDecorater(keyDescriptor('hash'), args)
}

/**
 * Mark DynamoDB range key
 */
export const rangeKey = (...args) => {
    return createDecorater(keyDescriptor('range'), args)
}

const indexDescriptor: (global?: boolean) => HandleDescriptor = global => (target, key, desc, [name]) => {
    log('indexDescriptor', target, key, desc, name)
    const index: any = { name: name || key }
    if (global) index.global = true
    metadataFor(target, key)['tdmo:index'] = index
    // required(target, key, desc)
}

/**
 * Mark DynamoDB global index
 */
export const globalIndex = (...args) => {
    return createDecorater(indexDescriptor(true), args)
}

/**
 * Mark DynamoDB local index
 */
export const localIndex = (...args) => {
    return createDecorater(indexDescriptor(), args)
}
