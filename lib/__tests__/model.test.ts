import dynalite from 'dynalite'
import { Model, $put } from '../model'
import { tableName, required, optional, hashKey, rangeKey, globalIndex, localIndex } from '../decorator'

const { AWS } = Model

Object.assign(process.env, {
    PORT: 45678,
    AWS_ACCESS_KEY_ID: 'AKID',
    AWS_SECRET_ACCESS_KEY: 'SECRET',
    AWS_REGION: 'cn-north-1',
})
const endpoint = `http://127.0.0.1:${process.env.PORT}`

AWS.config.update({
    dynamodb: {
        endpoint,
    },
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
        let dynaliteServer, dynamo: AWS.DynamoDB

        beforeEach(() => {
            return new Promise((resolve, reject) => {
                dynaliteServer = dynalite({
                    createTableMs: 0,
                    updateTableMs: 0,
                    deleteTableMs: 0,
                })
                dynaliteServer.listen(process.env.PORT, function (err) {
                    if (err) return reject(err)
                    // console.log('Dynalite started on port 45678')
                    dynamo = new AWS.DynamoDB()
                    resolve()
                })
            })
        })

        afterEach(() => {
            return new Promise((resolve, reject) => {
                dynaliteServer.close((err) => {
                    if (err) return reject(err)
                    // console.log('Dynalite closed')
                    resolve()
                })
            })
        })

        it('dynalite', async () => {
            let { TableNames } = await dynamo.listTables().promise()

            expect(TableNames.length).toBe(0)
        })

        it('create table', async () => {
            let { TableDescription } = await dynamo.createTable({
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

            let { Table } = await dynamo.describeTable({
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
                await dynamo.createTable({
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

        describe('get', () => {
            class Example extends Model {
                @required
                id: string
            }

            beforeEach(async () => {
                await dynamo.createTable({
                    TableName: 'Example',
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

            it('return undefined if not found', async () => {
                let e = await Example.get({ id: '1' })

                expect(e).toBeUndefined()
            })
        })

        describe('query', () => {
            // @tableName
            class Example extends Model {
                @hashKey id: string

                @required type: string
                @required name: string
                @required uid: string
                @optional len: number
                @optional age: number
            }

            beforeEach(async () => {
                await dynamo.createTable({
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
                let res = await Example.findOne().where('id').eq('1')

                expect(res.id).toBe('1')
            })

            it('query where id = 1', async () => {
                let res = await Example.find().where('id').eq('1')

                expect(res.length).toBe(1)
            })

            it('query uid = 1 and between a c', async () => {
                let res = await Example.find()
                    .index('uid-name-global')
                    .where('uid').eq('1')
                    .where('name').between(['a', 'c'])

                expect(res.length).toBe(1)
                expect(res[0].id).toBe('2')
            })

            it('query one from index uid-global', async () => {
                let m = await Example.findOne()
                    .index('uid-global')
                    .where('uid').eq('1')

                expect(m.id).toBe('2')

                let n = await Example.findOne()
                    .index('uid-global')
                    .where('uid').eq('-1')

                expect(n).toBeUndefined()
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
                }
                @optional pets?: {
                    name: string
                    age: number
                }[]
            }

            beforeEach(async () => {
                await dynamo.createTable({
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

        describe('remove', () => {
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
                await dynamo.createTable({
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

            it('remove with hash', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                let e = await Example.remove({ id: '4' })

                expect(e.name).toBe('joe')

                let a = await Example.find().where('id').eq('4')

                expect(a.length).toBe(0)
            })

            it('throw when remove with wrong condition', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                await expect(Example.remove({ id: '4' })
                    .where('age').gt(10)
                ).rejects.toThrow('The conditional request failed')

                let a = await Example.find().where('id').eq('4')

                expect(a.length).toBe(1)
            })

            it('remove instance', async () => {
                await Example.create({ id: '4', name: 'joe', age: 5, height: 50, weight: 30 })
                let e = await Example.get({ id: '4' })
                await e.remove()
                let a = await Example.find().where('id').eq('4')

                expect(a.length).toBe(0)
            })
        })

        xit('', async () => {
            await dynamo.createTable({
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
