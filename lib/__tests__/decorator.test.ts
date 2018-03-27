import { Model } from '../model'
import { tableName, required, optional, hashKey, rangeKey, globalIndex, localIndex } from '../decorator'

describe('decorator', () => {
    describe('tableName', () => {
        it('default class name', async () => {
            @tableName
            class Foo extends Model { }

            expect(Foo.tableName).toBe('Foo')
        })

        it('use class name if not set', async () => {
            @tableName()
            class Foo extends Model { }

            expect(Foo.tableName).toBe('Foo')
        })

        it('can set table name', async () => {
            @tableName('foos')
            class Foo extends Model { }

            expect(Foo.tableName).toBe('foos')
        })
    })

    describe('key', () => {
        it('mark hash key', () => {
            class Foo extends Model {
                @hashKey
                name: string
            }

            expect(Foo.metadata.name['tdmo:hash']).toBe(true)
        })

        it('mark range key', () => {
            class Foo extends Model {
                @rangeKey
                name: string
            }

            expect(Foo.metadata.name['tdmo:range']).toBe(true)
        })
    })

    describe('indexes', () => {
        it('set global index', () => {
            class Foo extends Model {
                @globalIndex
                name: string
            }

            expect(Foo.metadata.name['tdmo:index']).toEqual({ name: 'name', global: true })
        })

        it('set global index name', () => {
            class Foo extends Model {
                @globalIndex('name-global-index')
                name: string
            }

            expect(Foo.metadata.name['tdmo:index']).toEqual({ name: 'name-global-index', global: true })
        })

        it('set local index', () => {
            class Foo extends Model {
                @localIndex
                name: string
            }

            expect(Foo.metadata.name['tdmo:index']).toEqual({ name: 'name' })
        })

        it('set local index name', () => {
            class Foo extends Model {
                @localIndex('name-local-index')
                name: string
            }

            expect(Foo.metadata.name['tdmo:index']).toEqual({ name: 'name-local-index' })
        })
    })
})
