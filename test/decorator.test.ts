import { Model } from '../src/model'
import { tableName, required, optional, hashKey, rangeKey, globalIndex, localIndex, timestamp } from '../src/decorator'

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

        it('default class name if no decorator', async () => {
            class Foo extends Model { }

            expect(Foo.tableName).toBe('Foo')
        })
    })

    describe('key', () => {
        it('mark hash key', () => {
            class Foo extends Model {
                @hashKey
                name: string
            }

            expect(Foo.hashKey).toBe('name')
        })

        it('mark range key', () => {
            class Foo extends Model {
                @rangeKey
                name: string
            }

            expect(Foo.rangeKey).toBe('name')
        })

        it('hash/range key is required', () => {
            class Foo extends Model {
                @hashKey
                name: string

                @rangeKey
                age: number
            }

            expect(new Foo().validate().error.message).toMatch('"name" is required')
            expect(new Foo({ name: 'a' }).validate().error.message).toMatch('"age" is required')
            expect(new Foo({ name: 'a', age: 1 }).validate().error).toBeNull()
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

            expect(new Foo({})).toEqual({ name: 'n', label: 'l' })
        })
    })

    describe('indexes', () => {
        it('set global index', () => {
            class Foo extends Model {
                @globalIndex
                name: string
            }

            expect(Foo.globalIndexes).toEqual({
                'name-global': {
                    hashKey: 'name',
                },
            })
        })

        it('set global index name', () => {
            class Foo extends Model {
                @globalIndex({ name: 'nameGlobalIndex' })
                name: string
            }

            expect(Foo.globalIndexes).toEqual({
                'nameGlobalIndex': {
                    hashKey: 'name',
                },
            })
        })

        it('set composite global index', () => {
            class Foo extends Model {
                @globalIndex({ name: 'name-age-global' })
                name: string

                @globalIndex({ name: 'name-age-global', type: 'range' })
                age: number
            }

            expect(Foo.globalIndexes).toEqual({
                'name-age-global': {
                    hashKey: 'name',
                    rangeKey: 'age',
                },
            })
        })

        it('set local index', () => {
            class Foo extends Model {
                @localIndex
                name: string
            }

            expect(Foo.localIndexes).toEqual({
                'name-local': {
                    rangeKey: 'name',
                },
            })
        })

        it('set local index name', () => {
            class Foo extends Model {
                @localIndex({ name: 'nameLocalIndex' })
                name: string
            }

            expect(Foo.localIndexes).toEqual({
                'nameLocalIndex': {
                    rangeKey: 'name',
                },
            })
        })

        it('global/local index is optional', () => {
            class Foo extends Model {
                @globalIndex
                name: string

                @localIndex
                age: number
            }

            expect(new Foo().validate().error).toBeNull()
        })
    })

    describe('timestamp', () => {
        class Foo extends Model {
            @timestamp
            createdAt?: Date

            @timestamp({ type: 'update' })
            updatedAt?: Date

            @timestamp({ type: 'expire' })
            expiredAt?: number
        }

        it('mark timestamps', () => {
            let f = new Foo({})

            expect(f.createdAt).toMatch(/^201/)
            expect(f.updatedAt).toMatch(/^201/)
            expect(f.expiredAt).toBeUndefined()
        })

        it('create and update timestamp is iso 8601', () => {
            expect(new Foo({
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
            }).validate().error).toBeNull()

            expect(new Foo({
                createdAt: '123',
            }).validate().error).toBeTruthy()

            expect(new Foo({
                updatedAt: '123',
            }).validate().error).toBeTruthy()
        })

        it('expire timestamp is unix epoch', () => {
            expect(new Foo({
                expiredAt: Math.floor(Date.now() / 1000),
            }).validate().error).toBeNull()

            expect(new Foo({
                expiredAt: '123',
            }).validate().error).toBeNull()

            expect(new Foo({
                expiredAt: 0,
            }).validate().error).toBeTruthy()
        })
    })
})
