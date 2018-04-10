import { Query } from '../query'
import { Model } from '../model'
import { tableName } from '../decorator'

describe('Query', () => {
    describe('toJSON', () => {
        it('support cond set unset del', () => {
            let q = new Query({ TableName: 'users' })
                .index('id-global-index')
                .where('id').eq('123')
                .filter('name').eq('abc')
                .filter('age').gte(14)
                .filter('profile.tags[0]').eq('cool')
                .select('age')
                .limit(10)
                .sort(-1)

            expect(q.toJSON()).toEqual({
                TableName: 'users',
                IndexName: 'id-global-index',
                Limit: 10,
                ScanIndexForward: false,
                KeyConditionExpression: '#id = :id',
                FilterExpression: '#name = :name AND #age >= :age AND #profile.#tags[0] = :profile_tags_0',
                ProjectionExpression: '#age',
                ExpressionAttributeNames: {
                    '#id': 'id',
                    '#name': 'name',
                    '#age': 'age',
                    '#profile': 'profile',
                    '#tags': 'tags'
                },
                ExpressionAttributeValues: {
                    ':id': '123',
                    ':name': 'abc',
                    ':age': 14,
                    ':profile_tags_0': 'cool'
                }
            })
        })

        it('set Limit = 1 if one', () => {
            let q = new Query({ one: true })

            expect(q.toJSON()).toEqual({ Limit: 1 })
        })
    })

    describe('exprs', () => {
        it('begins or lt', () => {
            let o = new Query({ logic: 'OR' })
                .where('nickname').begins('a')
                .where('height').lt(100)
                .toJSON()

            expect(o.KeyConditionExpression).toBe('begins_with(#nickname, :nickname_begins_with) OR #height < :height')
            expect(o.ExpressionAttributeNames).toEqual({ '#nickname': 'nickname', '#height': 'height' })
            expect(o.ExpressionAttributeValues).toEqual({ ':nickname_begins_with': 'a', ':height': 100 })
        })

        it('or', () => {
            let o = new Query()
                .where('age').gte(18)
                .or(q => q
                    .where('height').lt(150)
                    .where('weight').lt(50)
                )
                .toJSON()

            expect(o.KeyConditionExpression).toBe('#age >= :age AND (#height < :height OR #weight < :weight)')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#height': 'height', '#weight': 'weight' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':height': 150, ':weight': 50 })
        })

        it('not', () => {
            let o = new Query()
                .where('age').gte(18)
                .not(q => q
                    .where('sex').eq('boy')
                    .where('ban').eq(true)
                )
                .toJSON()

            expect(o.KeyConditionExpression).toBe('#age >= :age AND (NOT #sex = :sex AND #ban = :ban)')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#sex': 'sex', '#ban': 'ban' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':sex': 'boy', ':ban': true })
        })

        it('not or', () => {
            let o = new Query()
                .where('id').eq(1)
                .not(q => q
                    .where('age').gte(18)
                    .or(q => q
                        .where('sex').eq('boy')
                        .where('ban').eq(true)
                    )
                )
                .toJSON()

            expect(o.KeyConditionExpression).toBe('#id = :id AND (NOT #age >= :age AND (#sex = :sex OR #ban = :ban))')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#ban': 'ban', '#id': 'id', '#sex': 'sex' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':ban': true, ':id': 1, ':sex': 'boy' })
        })
    })

    describe('options', () => {
        it('limit', () => {
            expect(new Query().limit(2).toJSON().Limit).toBe(2)
        })

        it('one', () => {
            expect(new Query().one().toJSON().Limit).toBe(1)
        })

        it('sort', () => {
            let sort = (order?) => new Query().sort(order).toJSON().ScanIndexForward

            expect(sort()).toBeUndefined()
            expect(sort(true)).toBeUndefined()
            expect(sort(1)).toBeUndefined()
            expect(sort('asc')).toBeUndefined()
            expect(sort(false)).toBe(false)
            expect(sort(-1)).toBe(false)
            expect(sort('desc')).toBe(false)
        })

        it('consistent', () => {
            expect(new Query().consistent().toJSON().ConsistentRead).toBe(true)
            expect(new Query().consistent(true).toJSON().ConsistentRead).toBe(true)
            expect(new Query().consistent(false).toJSON().ConsistentRead).toBeUndefined()
        })

        it('index', () => {
            expect(new Query().index('idx').toJSON().IndexName).toBe('idx')
            expect(new Query().index().toJSON().IndexName).toBeUndefined()
        })
    })
})
