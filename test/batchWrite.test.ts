import { BatchWrite } from '../src/batchWrite'
import { Model } from '../src/model'

describe('BatchWrite', () => {
    class Example extends Model { }

    describe('exprs', () => {
        it('put empty', () => {
            expect(new BatchWrite({ Model: Example }).toJSON().RequestItems.Example).toEqual([])
        })

        it('put items from arguments', () => {
            expect(new BatchWrite({ Model: Example, PutItems: [{}] }).toJSON().RequestItems.Example).toEqual([{ PutRequest: { Item: {} } }])
        })

        it('put items from method', () => {
            expect(new BatchWrite({ Model: Example }).put([{}]).toJSON().RequestItems.Example).toEqual([{ PutRequest: { Item: {} } }])
        })

        it('delete empty', () => {
            expect(new BatchWrite({ Model: Example }).toJSON().RequestItems.Example).toEqual([])
        })

        it('delete items from arguments', () => {
            expect(new BatchWrite({ Model: Example, DeleteKeys: [{}] }).toJSON().RequestItems.Example).toEqual([{ DeleteRequest: { Key: {} } }])
        })

        it('delete items from method', () => {
            expect(new BatchWrite({ Model: Example }).delete([{}]).toJSON().RequestItems.Example).toEqual([{ DeleteRequest: { Key: {} } }])
        })
    })

    it('is promise like', () => {
        let bg = new BatchWrite()

        expect(typeof bg.then).toBe('function')
        expect(typeof bg.catch).toBe('function')
    })

    it('is async iterable', () => {
        let it = new BatchWrite()[Symbol.asyncIterator]()

        expect(typeof it.next).toBe('function')
        expect(typeof it.throw).toBe('function')
        expect(typeof it.return).toBe('function')
    })
})
