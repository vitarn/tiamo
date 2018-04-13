import { Get } from '../src/get'

describe('Get', () => {
    describe('exprs', () => {
        it('select prop', () => {
            let o = new Get()
                .select('prop')
                .toJSON()

            expect(o).toEqual({
                ProjectionExpression: '#prop',
                ExpressionAttributeNames: {
                    '#prop': 'prop',
                },
            })
        })

        it('select name prop.sub[0]', () => {
            let o = new Get()
                .select('name prop.sub[0]')
                .toJSON()

            expect(o).toEqual({
                ProjectionExpression: '#name, #prop.#sub[0]',
                ExpressionAttributeNames: {
                    '#name': 'name',
                    '#prop': 'prop',
                    '#sub': 'sub',
                },
            })
        })

        it('select ["name", "prop.sub[0]"]', () => {
            let o = new Get()
                .select(['name', 'prop.sub[0]'])
                .toJSON()

            expect(o).toEqual({
                ProjectionExpression: '#name, #prop.#sub[0]',
                ExpressionAttributeNames: {
                    '#name': 'name',
                    '#prop': 'prop',
                    '#sub': 'sub',
                },
            })
        })
    })
})
