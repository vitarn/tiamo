import debug from './debug'
import { metadataFor } from 'tdv'

const log = debug('metadata')

const key = Symbol.for('__dynamodb__')

/**
 * Get dynamodb metadata for target class
 * @param target `prototype` of class function
 */
export function dynamodbFor(target: Object): { [name: string]: any } {
    const metadata = metadataFor(target)

    if (metadata.hasOwnProperty(key) === false) {
        Object.defineProperty(metadata, key, {
            // Defaults: NOT enumerable, configurable, or writable
            value: {}
        })
    }

    return metadata[key]
}
