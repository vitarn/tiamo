import { debuglog } from 'util'

let debug
try {
    debug = require('debug')
} catch (err) {
    debug = debuglog
}

export default (namespace: string) => debug(`tiamo:${namespace}`)
