import { Scan } from '../src/scan'
import { Model } from '../src/model'

describe('Scan', () => {
    describe('toJSON', () => {
        it('support filter and projection', () => {
            let q = new Scan({ TableName: 'users' })
                .filter('name').eq('abc')
                .filter('age').gte(14)
                .filter('profile.tags[0]').eq('cool')
                .select('string')
                .select('split1 split2')
                .select('arg1', 'arg2')
                .select(['index1', 'index2'])

            expect(q.toJSON()).toEqual({
                TableName: 'users',
                FilterExpression: '#name = :name AND #age >= :age AND #profile.#tags[0] = :profile_tags_0',
                ProjectionExpression: '#string, #split1, #split2, #arg1, #arg2, #index1, #index2',
                ExpressionAttributeNames: {
                    '#name': 'name',
                    '#age': 'age',
                    '#profile': 'profile',
                    '#tags': 'tags',
                    '#string': 'string',
                    '#split1': 'split1',
                    '#split2': 'split2',
                    '#arg1': 'arg1',
                    '#arg2': 'arg2',
                    '#index1': 'index1',
                    '#index2': 'index2',
                },
                ExpressionAttributeValues: {
                    ':name': 'abc',
                    ':age': 14,
                    ':profile_tags_0': 'cool'
                }
            })
        })
    })

    describe('exprs', () => {
        it('or', () => {
            let o = new Scan()
                .filter('age').gte(18)
                .or(q => q
                    .filter('height').lt(150)
                    .filter('weight').lt(50)
                )
                .toJSON()

            expect(o.FilterExpression).toBe('#age >= :age AND (#height < :height OR #weight < :weight)')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#height': 'height', '#weight': 'weight' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':height': 150, ':weight': 50 })
        })

        it('not', () => {
            let o = new Scan()
                .filter('age').gte(18)
                .not(q => q
                    .filter('sex').eq('boy')
                    .filter('ban').eq(true)
                )
                .toJSON()

            expect(o.FilterExpression).toBe('#age >= :age AND (NOT #sex = :sex AND #ban = :ban)')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#sex': 'sex', '#ban': 'ban' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':sex': 'boy', ':ban': true })
        })

        it('not or', () => {
            let o = new Scan()
                .filter('id').eq(1)
                .not(q => q
                    .filter('age').gte(18)
                    .or(q => q
                        .filter('sex').eq('boy')
                        .filter('ban').eq(true)
                    )
                )
                .toJSON()

            expect(o.FilterExpression).toBe('#id = :id AND (NOT #age >= :age AND (#sex = :sex OR #ban = :ban))')
            expect(o.ExpressionAttributeNames).toEqual({ '#age': 'age', '#ban': 'ban', '#id': 'id', '#sex': 'sex' })
            expect(o.ExpressionAttributeValues).toEqual({ ':age': 18, ':ban': true, ':id': 1, ':sex': 'boy' })
        })
    })
})
