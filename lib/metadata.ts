import { metadataFor } from 'tdv'
import { log } from './log'

const key = Symbol.for('__dynamodb__')

/**
 * Get dynamodb metadata for target class
 * @param target `prototype` of class function
 */
export function dynamodbFor(target: Object) {
    const metadata = metadataFor(target)

    if (metadata.hasOwnProperty(key) === false) {
        Object.defineProperty(metadata, key, {
            // Defaults: NOT enumerable, configurable, or writable
            value: {}
        })
    }

    return metadata[key]
}
