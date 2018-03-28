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

            expect(Foo.metadata.name['tdmo:index:global']).toEqual({
                name: 'name-global',
                type: 'hash',
            })

            expect(Foo.globalIndexes).toEqual([{
                name: 'name-global',
                hash: 'name',
            }])
        })

        it('set global index name', () => {
            class Foo extends Model {
                @globalIndex({ name: 'name-global-index' })
                name: string
            }

            expect(Foo.metadata.name['tdmo:index:global']).toEqual({
                name: 'name-global-index',
                type: 'hash',
            })

            expect(Foo.globalIndexes).toEqual([{
                name: 'name-global-index',
                hash: 'name',
            }])
        })

        it ('set global index hash and range', () => {
            class Foo extends Model {
                @globalIndex({ name: 'name-age-global' })
                name: string

                @globalIndex({ name: 'name-age-global', type: 'range' })
                age: number
            }

            expect(Foo.globalIndexes).toEqual([{
                name: 'name-age-global',
                hash: 'name',
                range: 'age',
            }])
        })

        it('set local index', () => {
            class Foo extends Model {
                @localIndex
                name: string
            }

            expect(Foo.metadata.name['tdmo:index:local']).toEqual({
                name: 'name-local',
                type: 'range',
            })

            expect(Foo.localIndexes).toEqual([{
                name: 'name-local',
                range: 'name',
            }])
        })

        it('set local index name', () => {
            class Foo extends Model {
                @localIndex({ name: 'name-local-index' })
                name: string
            }

            expect(Foo.metadata.name['tdmo:index:local']).toEqual({
                name: 'name-local-index',
                type: 'range',
            })

            expect(Foo.localIndexes).toEqual([{
                name: 'name-local-index',
                range: 'name',
            }])
        })
    })
})
