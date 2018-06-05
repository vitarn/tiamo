import { Database } from '../src/database'
import { hashKey, rangeKey, globalIndex, localIndex } from '../src/decorator'

describe('Database', () => {
    class Example extends Database {
        @hashKey h: string
        @rangeKey r: string
        @globalIndex({ name: 'global-index' }) gh: string
        @globalIndex({ name: 'global-index', type: 'range' }) gr: string
        @localIndex({ name: 'local-index' }) lr: string
    }
    let example = new Example({ h: 'h', r: 'r', gh: 'gh', gr: 'gr', lr: 'lr' })

    it('get primary key', () => {
        expect(example.getKey()).toEqual({
            h: 'h', r: 'r'
        })
    })

    it('get global index key', () => {
        expect(example.getKey('global-index')).toEqual({
            gh: 'gh', gr: 'gr'
        })
    })

    it('get local index key', () => {
        expect(example.getKey('local-index')).toEqual({
            h: 'h', lr: 'lr'
        })
    })
})
