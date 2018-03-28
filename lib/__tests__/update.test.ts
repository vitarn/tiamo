import { Update } from '../update'
import { createSet } from '../expression'

describe('Update', () => {
    describe('toJSON', () => {
        it('support cond set unset del', () => {
            let u = new Update({ Key: { id: '1' }, TableName: 'users' })
                .where('active').eq(true)
                .set('name').to('abc')
                .set('age').to(14)
                .set('profile.tags[0]').to('cool')
                .remove('ban', 'lock')
                .remove('inactive')
                .delete('roles', new Set(['admin']))
                .delete('profile.tags', new Set(['bad']))

            expect(u.toJSON()).toEqual({
                Key: {
                    id: '1',
                },
                TableName: 'users',
                ConditionExpression: '#active = :active',
                UpdateExpression: 'SET #name = :name, #age = :age, #profile.#tags[0] = :profile_tags_0 REMOVE #ban, #lock, #inactive DELETE #roles :roles_delete, #profile.#tags :profile_tags_delete',
                ExpressionAttributeNames: {
                    '#active': 'active',
                    '#name': 'name',
                    '#age': 'age',
                    '#profile': 'profile',
                    '#tags': 'tags',
                    '#ban': 'ban',
                    '#lock': 'lock',
                    '#inactive': 'inactive',
                    '#roles': 'roles',
                },
                ExpressionAttributeValues: {
                    ':active': true,
                    ':name': 'abc',
                    ':age': 14,
                    ':profile_tags_0': 'cool',
                    ':roles_delete': createSet(['admin']),
                    ':profile_tags_delete': createSet(['bad']),
                },
            })
        })
    })
})
