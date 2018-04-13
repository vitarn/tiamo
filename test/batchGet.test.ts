import { BatchGet } from '../src/batchGet'
import { Model } from '../src/model'

describe('BatchGet', () => {
    class Example extends Model { }

    describe('exprs', () => {
        it('select prop', () => {
            let o = new BatchGet({ Model: Example })
                .select('prop')
                .toJSON()

            expect(o.RequestItems.Example).toEqual({
                Keys: [],
                ProjectionExpression: '#prop',
                ExpressionAttributeNames: {
                    '#prop': 'prop',
                },
            })
        })

        it('select name prop.sub[0]', () => {
            let o = new BatchGet({ Model: Example })
                .select('name prop.sub[0]')
                .toJSON()

            expect(o.RequestItems.Example).toEqual({
                Keys: [],
                ProjectionExpression: '#name, #prop.#sub[0]',
                ExpressionAttributeNames: {
                    '#name': 'name',
                    '#prop': 'prop',
                    '#sub': 'sub',
                },
            })
        })

        it('select ["name", "prop.sub[0]"]', () => {
            let o = new BatchGet({ Model: Example })
                .select(['name', 'prop.sub[0]'])
                .toJSON()

            expect(o.RequestItems.Example).toEqual({
                Keys: [],
                ProjectionExpression: '#name, #prop.#sub[0]',
                ExpressionAttributeNames: {
                    '#name': 'name',
                    '#prop': 'prop',
                    '#sub': 'sub',
                },
            })
        })
    })

    it('is promise like', () => {
        let bg = new BatchGet()

        expect(typeof bg.then).toBe('function')
        expect(typeof bg.catch).toBe('function')
    })

    it('is async iterable', () => {
        let it = new BatchGet()[Symbol.asyncIterator]()

        expect(typeof it.next).toBe('function')
        expect(typeof it.throw).toBe('function')
        expect(typeof it.return).toBe('function')
    })
})
