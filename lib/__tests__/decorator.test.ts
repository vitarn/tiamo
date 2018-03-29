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

            expect(Foo.metadata.name['tiamo:hash']).toBe(true)
        })

        it('mark range key', () => {
            class Foo extends Model {
                @rangeKey
                name: string
            }

            expect(Foo.metadata.name['tiamo:range']).toBe(true)
        })

        it('hash/range key is required', () => {
            class Foo extends Model {
                @hashKey
                name: string

                @rangeKey
                age: number
            }

            expect(() => new Foo().attempt()).toThrow('"name" is required')
            expect(() => new Foo({ name: 'a' }).attempt()).toThrow('"age" is required')
            expect(() => new Foo({ name: 'a', age: 1 }).attempt()).not.toThrow()
        })

        it('dont rewrite tdv metadata', () => {
            class Foo extends Model {
                @hashKey
                @optional(j => j.string().default('n'))
                name: string

                @optional(j => j.string().default('l'))
                @hashKey
                label: string
            }

            expect(new Foo().attempt()).toEqual({ name: 'n', label: 'l' })
        })
    })

    describe('indexes', () => {
        it('set global index', () => {
            class Foo extends Model {
                @globalIndex
                name: string
            }

            expect(Foo.metadata.name['tiamo:index:global']).toEqual({
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

            expect(Foo.metadata.name['tiamo:index:global']).toEqual({
                name: 'name-global-index',
                type: 'hash',
            })

            expect(Foo.globalIndexes).toEqual([{
                name: 'name-global-index',
                hash: 'name',
            }])
        })

        it('set composite global index', () => {
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

            expect(Foo.metadata.name['tiamo:index:local']).toEqual({
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

            expect(Foo.metadata.name['tiamo:index:local']).toEqual({
                name: 'name-local-index',
                type: 'range',
            })

            expect(Foo.localIndexes).toEqual([{
                name: 'name-local-index',
                range: 'name',
            }])
        })

        it('global/local index is optional', () => {
            class Foo extends Model {
                @globalIndex
                name: string

                @localIndex
                age: number
            }

            expect(() => new Foo().attempt()).not.toThrow()
        })
    })
})
