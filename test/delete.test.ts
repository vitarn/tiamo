import { Delete } from '../src/delete'
import { createSet } from '../src/expression'

describe('Delete', () => {
    describe('toJSON', () => {
        it('support cond set unset del', () => {
            let d = new Date(Date.now() - 24 * 60 * 60 * 1000)
            let u = new Delete({ Key: { id: '1' }, TableName: 'users' })
                .where('active').not.exists()
                .where('createdAt').lt(d as any)

            expect(u.toJSON()).toEqual({
                Key: {
                    id: '1',
                },
                TableName: 'users',
                ConditionExpression: 'attribute_not_exists(#active) AND #createdAt < :createdAt',
                ExpressionAttributeNames: {
                    '#active': 'active',
                    '#createdAt': 'createdAt',
                },
                ExpressionAttributeValues: {
                    ':createdAt': d,
                },
            })
        })
    })
})
