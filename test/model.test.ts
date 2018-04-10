import dynalite from 'dynalite'
import listen from 'test-listen'
import { Model, $batchGet, $batchWrite, $put, $get, $scan } from '../lib/model'
import { tableName, required, optional, hashKey, rangeKey, globalIndex, localIndex } from '../lib/decorator'

const { AWS } = Model

Object.assign(process.env, {
    AWS_ACCESS_KEY_ID: 'AKID',
    AWS_SECRET_ACCESS_KEY: 'SECRET',
    AWS_REGION: 'cn-north-1',
})

describe('Model', () => {
    describe('validate', () => {
        class Foo extends Model {
            @optional(j => j.string()
                .default(() => Math.random().toString(), 'random')
            )
            id: string
        }

        it('validate before save', async () => {
            await expect(Foo.create({ id: 1 as any })).rejects.toThrow()
        })
    })

    describe('dynamodb', () => {
        let dynaliteServer

        beforeEach(async () => {
            dynaliteServer = dynalite({
                createTableMs: 0,
                updateTableMs: 0,
                deleteTableMs: 0,
            })
            const endpoint: string = await listen(dynaliteServer)
            Model.ddb = new AWS.DynamoDB({ endpoint })
            Model.client = new AWS.DynamoDB.DocumentClient({ service: Model.ddb })
        })

        afterEach(() => {
            dynaliteServer.close() // dont wait bcs port is random
        })

        it('dynalite', async () => {
            let { TableNames } = await Model.ddb.listTables().promise()

            expect(TableNames.length).toBe(0)
        })

        it('create table', async () => {
            let { TableDescription } = await Model.ddb.createTable({
                TableName: 'test',
                AttributeDefinitions: [{
                    AttributeName: 'name',
                    AttributeType: 'S',
                }],
                KeySchema: [{
                    AttributeName: 'name',
                    KeyType: 'HASH',
                }],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1,
                }
            } as AWS.DynamoDB.CreateTableInput).promise()

            expect(TableDescription.TableName).toBe('test')

            let { Table } = await Model.ddb.describeTable({
                TableName: 'test',
            }).promise()

            expect(Table.TableStatus).toBe('ACTIVE')
        })

        describe('save', () => {
            @tableName
            class Foo extends Model {
                @required
                id: string
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'Foo',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'S',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    }
                } as AWS.DynamoDB.CreateTableInput).promise()
            })

            it('save into db', async () => {
                await Foo.create({ id: '1' })

                let foo = await Foo.get({ id: '1' })

                expect(foo.id).toBe('1')
            })
        })

        describe('put', () => {
            class PutExample extends Model {
                @hashKey
                id: number

                @optional
                name?: string

                @optional
                obj?: any

                @optional
                arr?: string[]
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'PutExample',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'N',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    }
                } as AWS.DynamoDB.CreateTableInput).promise()
            })

            it('put item into db', async () => {
                let e = await PutExample.put({ id: 1 })

                e = await PutExample.get({ id: 1 })

                expect(e.id).toBe(1)
            })

            it('put item overwrite existed item', async () => {
                let e = await PutExample.put({ id: 1, name: 'abc' })
                e = await PutExample.put({ id: 1, name: 'def' })

                e = await PutExample.get({ id: 1 })

                expect(e).toEqual({ id: 1, name: 'def' })
            })

            it('put item return value merge Item into old', async () => {
                let e = await PutExample.put({ id: 1 })

                expect(e).toEqual({ id: 1 })

                e = await PutExample.put({ id: 1, name: 'abc' })

                expect(e).toEqual({ id: 1, name: 'abc' })
            })

            it('put item if cond is true', async () => {
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('id').not.exists()).resolves.toEqual({ id: 1, name: 'abc' })

                await expect(PutExample.put({ id: 1, name: 'abc' }).where('id').exists()).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('id').eq(1)).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('id').gte(1)).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').begins('a')).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').between(['a', 'b'])).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').contains('c')).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').gte('a')).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').in(['abc', 'def'])).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').type('S')).resolves.toEqual({ id: 1, name: 'abc' })
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('name').size.eq(3)).resolves.toEqual({ id: 1, name: 'abc' })
            })

            it('throw when put item cond fail', async () => {
                await expect(PutExample.put({ id: 1, name: 'abc' }).where('id').not.exists()).resolves.toEqual({ id: 1, name: 'abc' })

                await expect(PutExample.put({ id: 1 }).where('id').not.exists()).rejects.toThrow('The conditional request failed')
                await expect(PutExample.put({ id: 1 }).where('id').ne(1)).rejects.toThrow('The conditional request failed')
                await expect(PutExample.put({ id: 1 }).where('id').gt(1)).rejects.toThrow('The conditional request failed')
                await expect(PutExample.put({ id: 1 }).where('name').begins('z')).rejects.toThrow('The conditional request failed')
            })
        })

        describe('get', () => {
            class GetExample extends Model {
                @required
                id: string

                @optional
                name?: string

                @optional
                obj?: any

                @optional
                arr?: string[]
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'GetExample',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'S',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    }
                } as AWS.DynamoDB.CreateTableInput).promise()

                await GetExample.create({ id: '1', name: '1', arr: ['1'] })
                await GetExample.create({ id: '2', name: '1', arr: ['2'] })
            })

            it('return undefined if not found', async () => {
                let e = await GetExample.get({ id: '0' })

                expect(e).toBeUndefined()
            })

            it('get id = 1', async () => {
                let e = await GetExample.get({ id: '1' })

                expect(e.id).toBe('1')
            })

            it('get id = 1 select arr', async () => {
                let e = await GetExample.get({ id: '1' })
                    .select('arr[0]')

                expect(e.name).toBeUndefined()
                expect(e.arr[0]).toBe('1')
            })
        })

        describe('query', () => {
            class Example extends Model {
                @hashKey id: string

                @required type: string
                @required name: string
                @required uid: string
                @optional len: number
                @optional age: number
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'Example',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'S',
                    }, {
                        AttributeName: 'uid',
                        AttributeType: 'S',
                    }, {
                        AttributeName: 'name',
                        AttributeType: 'S',
                    }, {
                        AttributeName: 'type',
                        AttributeType: 'S',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    },
                    GlobalSecondaryIndexes: [{
                        IndexName: 'uid-global',
                        KeySchema: [{
                            AttributeName: 'uid',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'name',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }, {
                        IndexName: 'uid-name-global',
                        KeySchema: [{
                            AttributeName: 'uid',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'name',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }, {
                        IndexName: 'uid-type-global',
                        KeySchema: [{
                            AttributeName: 'uid',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'type',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }],
                }).promise()

                await Promise.all([
                    Example.create({ id: '1', name: 'foo', uid: '1', type: 'video', len: 30, age: 4 }),
                    Example.create({ id: '2', name: 'bar', uid: '1', type: 'audio', len: 60, age: 10 }),
                    Example.create({ id: '3', name: 'zoo', uid: '2', type: 'video', len: 90, age: 14 }),
                ])
            })

            it('query one where id = 1', async () => {
                let res = await Example.query().one().where('id').eq('1')

                expect(res.id).toBe('1')
            })

            it('query where id = 1', async () => {
                let res = await Example.query().where('id').eq('1')

                expect(res.length).toBe(1)
            })

            it('query uid = 1 and between a c', async () => {
                let res = await Example.query()
                    .index('uid-name-global')
                    .where('uid').eq('1')
                    .where('name').between(['a', 'c'])

                expect(res.length).toBe(1)
                expect(res[0].id).toBe('2')
            })

            it('query one from index uid-global', async () => {
                let m = await Example.query().one()
                    .index('uid-global')
                    .where('uid').eq('1')

                expect(m.id).toBe('2')

                let n = await Example.query().one()
                    .index('uid-global')
                    .where('uid').eq('-1')

                expect(n).toBeUndefined()
            })

            it('query one model', async () => {
                let m = await Example.query().one()
                    .where('id').eq('1')

                expect(m.id).toBe('1')
            })
        })

        describe('scan', () => {
            class ScanExample extends Model {
                @hashKey id: number

                @optional data?: any
            }

            const ids = Array(5).fill(0).map((_, i) => i + 1)
            const data = {
                a: '0'.repeat(399).repeat(1024), // // 399KB
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'ScanExample',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'N',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1000,
                        WriteCapacityUnits: 1000,
                    },
                }).promise()

                await Promise.all(ids.map(id => ScanExample.create({ id, data })))
            })

            it('$scan is a async generator and multi times yield', async () => {
                let i = 0, a = []
                for await (let r of ScanExample[$scan]({})) {
                    i++
                    a = a.concat(r)
                }

                expect(i).toBe(2)
                expect(a.length).toBe(5)
            })

            it('scan full table', async () => {
                let scanIds = []
                for await (let r of ScanExample.scan()) {
                    scanIds.push(r.id)
                }

                expect(scanIds.sort()).toEqual(ids)

                scanIds = (await ScanExample.scan()).map(e => e.id).sort()

                expect(scanIds.sort()).toEqual(ids)
            })

            it('scan by id > 3', async () => {
                let scanIds = (await ScanExample.scan().filter('id').gt(3)).map(e => e.id).sort()

                expect(scanIds.sort()).toEqual([4, 5])
            })

            it('scan count', async () => {
                let c = await ScanExample.scan().filter('id').gt(3).count()

                expect(c).toBe(2)
            })
        })

        describe('update', () => {
            // @tableName
            class Example extends Model {
                @hashKey id: string

                @required name: string
                @required age: number
                @required height: number
                @required weight: number
                @optional roles?: string[]
                @optional profile?: {
                    displayName: string
                    phone: number
                    address: string
                    wechat: {
                        openId: string
                        name: string
                    }
                }
                @optional pets?: {
                    name: string
                    age: number
                }[]
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'Example',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'S',
                    }, {
                        AttributeName: 'age',
                        AttributeType: 'N',
                    }, {
                        AttributeName: 'height',
                        AttributeType: 'N',
                    }, {
                        AttributeName: 'weight',
                        AttributeType: 'N',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    },
                    GlobalSecondaryIndexes: [{
                        IndexName: 'age-height',
                        KeySchema: [{
                            AttributeName: 'age',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'height',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }, {
                        IndexName: 'age-weight',
                        KeySchema: [{
                            AttributeName: 'age',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'weight',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }],
                }).promise()

                await Promise.all([
                    Example.create({ id: '1', name: 'foo', age: 1, height: 30, weight: 10 }),
                    Example.create({ id: '2', name: 'bar', age: 10, height: 120, weight: 30 }),
                    Example.create({ id: '3', name: 'zoo', age: 10, height: 130, weight: 35 }),
                ])
            })

            it('update id = 1 set age = 2, height = 40, weight = 15', async () => {
                let e = await Example.update({ id: '1' })
                    .set('age').to(2)
                    .set('height').to(40)
                    .set('weight').to(15)
                    .set('unknown').to('unknown')

                expect(e.age).toBe(2)
            })

            it('update id = 1 set weight + 1', async () => {
                let e = await Example.update({ id: '1' })
                    .set('weight').plus(1)

                expect(e.weight).toBe(11)
            })

            it('update id = 1 set weight - 1', async () => {
                let e = await Example.update({ id: '1' })
                    .set('weight').minus(1)

                expect(e.weight).toBe(9)
            })

            it('update id = 1 set roles list_append [user]', async () => {
                let e = await Example.update({ id: '1' })
                    .set('roles').to(['user'])

                expect(e.roles).toEqual(['user'])

                e = await Example.update({ id: '1' })
                    .set('roles').append(['vip'])

                expect(e.roles).toEqual(['user', 'vip'])

                e = await Example.update({ id: '1' })
                    .set('roles').prepend(['gold'])

                expect(e.roles).toEqual(['gold', 'user', 'vip'])
            })

            it('update id = 1 set roles = [user] if not exists', async () => {
                let e = await Example.update({ id: '1' })
                    .set('roles').ifNotExists(['user'])

                expect(e.roles).toEqual(['user'])

                e = await Example.update({ id: '1' })
                    .set('roles').ifNotExists(['again'])

                expect(e.roles).toEqual(['user'])
            })

            it('update id = 1 set pets[0].age += 1', async () => {
                let e = await Example.update({ id: '1' })
                    .set('pets').to([{ name: 'poly', age: 16 }])
                e = await Example.update({ id: '1' })
                    .set('pets[0].age').plus(1)

                expect(e.pets[0].age).toBe(17)
            })

            it('update id = 1 set wechat name', async () => {
                let e = await Example.update({ id: '1' })
                    .where('id').exists()
                    .set('profile').ifNotExists({})
                e = await Example.update({ id: '1' })
                    .where('id').exists()
                    .set('profile.wechat').ifNotExists({
                        name: 'foo',
                    })

                expect(e.profile.wechat.name).toBe('foo')
            })

            it('update id = 1 remove weight', async () => {
                let e = await Example.update({ id: '1' })
                    .remove('weight')

                expect(e.weight).toBeUndefined()
            })

            it('update id = 1 remove roles', async () => {
                let e = await Example.update({ id: '1' })
                    .set('roles').to(['user', 'vip'])
                e = await Example.update({ id: '1' })
                    .remove('roles[1]')

                expect(e.roles).toEqual(['user'])
            })

            it('update id = 1 add roles vip', async () => {
                let e = await Example.update({ id: '1' })
                    .set('roles').to(new Set(['user']))
                e = await Example.update({ id: '1' })
                    .add('roles', new Set(['vip']))

                expect(e.roles).toEqual({
                    values: ['user', 'vip'],
                    type: 'String',
                    wrapperName: 'Set',
                })
            })

            it('update id = 1 delete roles vip', async () => {
                let e = await Example.update({ id: '1' })
                    .set('roles').to(new Set(['user', 'vip']))
                e = await Example.update({ id: '1' })
                    .delete('roles', new Set(['vip']))

                expect(e.roles).toEqual({
                    values: ['user'],
                    type: 'String',
                    wrapperName: 'Set',
                })
            })

            it('update id = 1 quiet', async () => {
                let e = await Example.update({ id: '1' })
                    .set('weight').to(9)
                    .quiet()

                expect(e).toBeUndefined()
            })

            it('update id = 1 nothing', async () => {
                await expect(Example.update({ id: '1' })).rejects.toThrow('Update expression is empty')
            })
        })

        describe('delete', () => {
            class Example extends Model {
                @hashKey id: string

                @required name: string
                @required age: number
                @required height: number
                @required weight: number
                @optional roles?: string[]
                @optional profile?: {
                    displayName: string
                    phone: number
                    address: string
                }
                @optional pets?: {
                    name: string
                    age: number
                }[]
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'Example',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'S',
                    }, {
                        AttributeName: 'age',
                        AttributeType: 'N',
                    }, {
                        AttributeName: 'height',
                        AttributeType: 'N',
                    }, {
                        AttributeName: 'weight',
                        AttributeType: 'N',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    },
                    GlobalSecondaryIndexes: [{
                        IndexName: 'age-height',
                        KeySchema: [{
                            AttributeName: 'age',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'height',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }, {
                        IndexName: 'age-weight',
                        KeySchema: [{
                            AttributeName: 'age',
                            KeyType: 'HASH',
                        }, {
                            AttributeName: 'weight',
                            KeyType: 'RANGE',
                        }],
                        Projection: {
                            ProjectionType: 'ALL',
                        },
                        ProvisionedThroughput: {
                            ReadCapacityUnits: 1,
                            WriteCapacityUnits: 1,
                        },
                    }],
                }).promise()

                await Promise.all([
                    Example.create({ id: '1', name: 'foo', age: 1, height: 30, weight: 10 }),
                    Example.create({ id: '2', name: 'bar', age: 10, height: 120, weight: 30 }),
                    Example.create({ id: '3', name: 'zoo', age: 10, height: 130, weight: 35 }),
                ])
            })

            it('delete with hash', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                let e = await Example.delete({ id: '4' })

                expect(e.name).toBe('joe')

                let a = await Example.query().where('id').eq('4')

                expect(a.length).toBe(0)
            })

            it('throw when delete with wrong condition', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                await expect(Example.delete({ id: '4' })
                    .where('age').gt(10)
                ).rejects.toThrow('The conditional request failed')

                let a = await Example.query().where('id').eq('4')

                expect(a.length).toBe(1)
            })

            it('delete instance', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                let e = await Example.get({ id: '4' })
                await e.delete()
                let a = await Example.query().where('id').eq('4')

                expect(a.length).toBe(0)
            })
        })

        describe('batch', () => {
            class BatchExample extends Model {
                @hashKey id: number

                @optional data?: any
            }

            const ids = Array(5).fill(0).map((_, i) => i + 1)
            const data = {
                a: '0'.repeat(399).repeat(1024), // // 399KB
            }

            beforeEach(async () => {
                await Model.ddb.createTable({
                    TableName: 'BatchExample',
                    AttributeDefinitions: [{
                        AttributeName: 'id',
                        AttributeType: 'N',
                    }],
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1000,
                        WriteCapacityUnits: 1000,
                    },
                }).promise()
            })

            it('$batchGet is a async generator and multi times yield', async () => {
                await Promise.all(ids.map(id => BatchExample.create({ id, data })))
                let i = 0, a = []
                for await (let r of BatchExample[$batchGet]({
                    RequestItems: {
                        BatchExample: {
                            Keys: ids.map(id => ({ id }))
                        }
                    }
                })) {
                    i++
                    a = a.concat(r.BatchExample)
                }

                expect(i).toBe(2)
                expect(a.length).toBe(5)
            })

            it('$batchWrite is a async generator', async () => {
                let i = 0
                let a = []
                for await (let r of BatchExample[$batchWrite]({
                    RequestItems: {
                        BatchExample: ids.map(id => ({ PutRequest: { Item: { id, data: {} } } })),
                    }
                })) {
                    i++
                    a = a.concat(r)
                }

                // dynalite not response if batchWrite too big.
                expect(i).toBe(1)
                expect(a.length).toBe(1)

                let e3 = await BatchExample.get({ id: 3 })
                expect(e3).toBeTruthy()

                i = 0
                a = []
                for await (let r of BatchExample[$batchWrite]({
                    RequestItems: {
                        BatchExample: ids.map(id => ({ DeleteRequest: { Key: { id } } })),
                    }
                })) {
                    i++
                    a = a.concat(r)
                }

                expect(i).toBe(1)
                expect(a.length).toBe(1)

                let e1 = await BatchExample.get({ id: 1 })
                expect(e1).toBeUndefined()
            })

            it('batch get await items', async () => {
                await BatchExample.create({ id: 1 })
                await BatchExample.create({ id: 2 })

                let result = await BatchExample.batch()
                    .get([{ id: 1 }, { id: 2 }, { id: 3 }])

                expect(result.length).toBe(2)
                result.forEach(r => expect(r).toBeInstanceOf(BatchExample))
            })

            it('batch get for await items', async () => {
                await BatchExample.create({ id: 1 })
                await BatchExample.create({ id: 2 })

                let it = BatchExample.batch()
                    .get([{ id: 1 }, { id: 2 }, { id: 3 }])

                for await (let e of it) {
                    expect(e).toBeInstanceOf(BatchExample)
                }
            })

            it('batch get select item props', async () => {
                await BatchExample.create({ id: 1, data: { a: 1, b: 1 } })
                await BatchExample.create({ id: 2, data: { a: 2, b: 2 } })

                let it = BatchExample.batch()
                    .get([{ id: 1 }, { id: 2 }, { id: 3 }])
                    .select('data.a')

                for await (let e of it) {
                    expect(e).toBeInstanceOf(BatchExample)
                    expect(e.id).toBeUndefined()
                    expect(e.data.a).toBeTruthy()
                    expect(e.data.b).toBeUndefined()
                }
            })

            it('batch put items', async () => {
                let it = BatchExample.batch()
                    .put([{ id: 1 }, { id: 2 }])

                for await (let e of it) { }

                await expect(BatchExample.get({ id: 1 })).resolves.toEqual({ id: 1 })
                await expect(BatchExample.get({ id: 2 })).resolves.toEqual({ id: 2 })
            })

            it('batch delete items', async () => {
                await BatchExample.create({ id: 1 })
                await BatchExample.create({ id: 2 })
                let it = BatchExample.batch()
                    .delete([{ id: 1 }, { id: 2 }])

                for await (let e of it) { }

                await expect(BatchExample.get({ id: 1 })).resolves.toBeUndefined()
                await expect(BatchExample.get({ id: 2 })).resolves.toBeUndefined()
            })

            it('batch put items then delete', async () => {
                await BatchExample.create({ id: 1 })
                await BatchExample.create({ id: 2 })
                await BatchExample.create({ id: 3 })
                let it = BatchExample.batch()
                    .put([{ id: 1 }, { id: 2 }])
                    .delete([{ id: 3 }])

                for await (let e of it) { }

                await expect(BatchExample.get({ id: 1 })).resolves.toEqual({ id: 1 })
                await expect(BatchExample.get({ id: 2 })).resolves.toEqual({ id: 2 })
                await expect(BatchExample.get({ id: 3 })).resolves.toBeUndefined()
            })

            it('throw if put and delete have same key', async () => {
                let p = BatchExample.batch()
                    .put([{ id: 1 }])
                    .delete([{ id: 1 }])

                await expect(p).rejects.toThrow('Provided list of item keys contains duplicates')
            })

            it('batch put delete and get', async () => {
                await BatchExample.create({ id: 1 })
                await BatchExample.create({ id: 2 })
                await BatchExample.create({ id: 3 })
                let p = BatchExample.batch()
                    .put([{ id: 1 }, { id: 2 }])
                    .delete([{ id: 3 }])
                    .get([{ id: 1 }, { id: 2 }])

                await expect(p).resolves.toHaveLength(2)
            })
        })

        xit('', async () => {
            await Model.ddb.createTable({
                TableName: 'Example',
                AttributeDefinitions: [{
                    AttributeName: 'id',
                    AttributeType: 'S',
                }, {
                    AttributeName: 'name',
                    AttributeType: 'S',
                }, {
                    AttributeName: 'age',
                    AttributeType: 'N',
                }, {
                    AttributeName: 'height',
                    AttributeType: 'N',
                }, {
                    AttributeName: 'weight',
                    AttributeType: 'N',
                }],
                KeySchema: [{
                    AttributeName: 'id',
                    KeyType: 'HASH',
                }, {
                    AttributeName: 'name',
                    KeyType: 'RANGE',
                }],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1,
                },
                GlobalSecondaryIndexes: [{
                    IndexName: 'age',
                    KeySchema: [{
                        AttributeName: 'age',
                        KeyType: 'HASH',
                    }],
                    Projection: {
                        ProjectionType: 'ALL',
                    },
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1,
                    },
                }],
                LocalSecondaryIndexes: [{
                    IndexName: 'age',
                    KeySchema: [{
                        AttributeName: 'id',
                        KeyType: 'HASH',
                    }, {
                        AttributeName: 'age',
                        KeyType: 'RANGE',
                    }],
                    Projection: {
                        ProjectionType: 'ALL',
                    },
                }],
            }).promise()
        })
    })
})
