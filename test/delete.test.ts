import { Delete } from '../lib/delete'
import { createSet } from '../lib/expression'

describe('Delete', () => {
    describe('toJSON', () => {
        it('support cond set unset del', () => {
            let d = new Date(Date.now() - 24 * 60 * 60 * 1000)
            let u = new Delete({ Key: { id: '1' }, TableName: 'users' })
                .where('active').not.exists()
                .where('createdAt').lt(d)

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
