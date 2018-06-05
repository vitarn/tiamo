import debug from './debug'
import { createDecorator, required, optional, HandleDescriptor, FlexibleDecorator} from 'tdv'
// import { modelMetaFor } from './metadata'
import { Model } from './model'

const log = debug('decorator')

export { required, optional } from 'tdv'

/**
 * Set DynamoDB table name for `Model` class
 * 
 *      `@tableName class Foo extends Model {} // Foo.tableName === 'Foo'`
 *      `@tableName('foos') class Foo extends Model {} // Foo.tableName === 'foos'`
 */
export const tableName: FlexibleDecorator<string> = (...args) => {
    return createDecorator((target, [name]) => {
        log('tableDescriptor', target, name)

        Reflect.defineMetadata('tiamo:table:name', name || target.name, target)
    }, args)
}

const keyDescriptor: (type: 'hash' | 'range') => HandleDescriptor = type => (target, key, desc) => {
    log('keyDescriptor', target, key, desc)

    // Dont rewrite existed tdv metadata
    if (!Reflect.hasOwnMetadata(`tdv:key:${key.toString()}`, target)) {
        required(target, key, desc)
    }

    Reflect.defineMetadata(`tiamo:table:${type}`, key, target)
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
    log('indexDescriptor', target, key, desc, options)

    const scope = global ? 'global' : 'local'
    const opts: IndexKeyOptions = options || {}
    const name = opts.name || `${key.toString()}-${scope}`
    const type = opts.type || (global ? 'hash' : 'range')

    // Dont rewrite existed tdv metadata
    if (!Reflect.hasOwnMetadata(`tdv:key:${key.toString()}`, target)) {
        optional(target, key, desc)
    }

    Reflect.defineMetadata(`tiamo:table:index:${scope}:${name}:${type}`, key, target)
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

const timestampDescriptor: HandleDescriptor = (target, key, desc, [opts]) => {
    const { type } = (opts || { type: 'create' }) as TimestampOptions
    log('timestampDescriptor', target, key, desc, type)

    // Dont rewrite existed tdv metadata
    if (!Reflect.hasOwnMetadata(`tdv:key:${key.toString()}`, target)) {
        if (~['create', 'update'].indexOf(type)) {
            optional(j => j.date()
                .iso()
                .default(
                    () => new Date().toISOString(),
                    `${type} iso 8601 timestamp`
                )
                .tags(['timestamp'])
                .example('1970-01-01T00:00:00.000Z')
            )(target, key, desc)
        } else if (type === 'expire') {
            optional(j => j.number()
                .integer()
                .positive()
                .description(`${type} unix epoch timestamp`)
                .tags(['timestamp'])
                .example('946684800000')
                .unit('seconds')
            )(target, key, desc)
        }
    }

    Reflect.defineMetadata(`tiamo:timestamp:${type}`, key, target)
}

/**
 * Mark timestamp field
 */
export const timestamp: FlexibleDecorator<TimestampOptions> = (...args) => {
    return createDecorator(timestampDescriptor, args)
}

export type TimestampOptions = {
    type?: 'create' | 'update' | 'expire'
}

/**
 * Modify the enumerable property of the property descriptor.
 */
export const enumerable: FlexibleDecorator<boolean> = (...args) => {
    return createDecorator((target, key, desc, [opts]) => {
        desc.enumerable = false
    }, args)
}
