import 'reflect-metadata'
import { Hook } from '../src/hook'

describe('Hook', () => {
    it('wrap function', async () => {
        let mock = jest.fn()
        let hook = new Hook()

        hook.pre('save', () => mock('pre:save:1'))
        hook.pre('save', () => mock('pre:save:2'))
        hook.post('save', () => mock('post:save:1'))
        hook.wrap('save', () => {
            mock('save')
        })()
        
        expect(mock).toHaveBeenCalledTimes(4)
        expect(mock).toBeCalledWith('pre:save:1')
        expect(mock).toBeCalledWith('pre:save:2')
        expect(mock).toBeCalledWith('post:save:1')
        expect(mock).toBeCalledWith('save')
    })

    describe('class method', () => {
        let preMock: jest.Mock, postMock: jest.Mock

        class Schema {
            // ['constructor']: typeof Schema & { new(...args): Schema }
            constructor() {
                this.pre('validate', () => this.cast())
            }

            protected get hook() {
                if (Reflect.hasOwnMetadata('hook', this)) {
                    return Reflect.getOwnMetadata('hook', this)
                }
                
                const hook = new Hook()

                Reflect.defineMetadata('hook', hook, this)

                return hook
            }

            pre(name: string, fn: Function) {
                return this.hook.pre(name, fn)
            }

            post(name: string, fn: Function) {
                return this.hook.post(name, fn)
            }

            cast() {
                preMock('cast')
            }

            validate = this.hook.wrap('validate', (opts = {}) => preMock('validate'))
        }

        class Model extends Schema {
            constructor() {
                super()
                this.pre('save', () => this.validate())
            }

            save = this.hook.wrap('save', (opts = {}) => Promise.resolve().then(() => preMock('save')))
        }

        class User extends Model {
            constructor() {
                super()
                this.pre('follow', () => this.save())
                this.post('follow', () => postMock('post:follow:sync'))
                this.post('follow', () => new Promise(setImmediate).then(() => postMock('post:follow:async')))
            }

            follow = this.hook.wrap('follow', (opts = {}) => preMock('follow'))
        }

        beforeEach(() => {
            preMock = jest.fn()
                .mockName('pre mock')
            postMock = jest.fn()
                .mockName('post mock')
        })

        it('Schema#validate is sync method', () => {
            expect(new Schema().validate()).toBeUndefined()
        })

        it('Model#save is async method', () => {
            expect(new Model().save().then).toBeInstanceOf(Function)
        })

        it('User#follow is async method', () => {
            expect(new User().follow().then).toBeInstanceOf(Function)
        })

        it('exec 2 sync pre hooks', () => {
            expect(new User().follow().then).toBeInstanceOf(Function)
            expect(preMock).toHaveBeenCalledTimes(2)
            expect(preMock).toHaveBeenCalledWith('cast')
            expect(preMock).toHaveBeenCalledWith('validate')
        })

        it('exec 4 async pre hooks', async () => {
            await new User().follow()

            expect(preMock).toHaveBeenCalledTimes(4)
            expect(preMock).toHaveBeenCalledWith('cast')
            expect(preMock).toHaveBeenCalledWith('validate')
            expect(preMock).toHaveBeenCalledWith('save')
            expect(preMock).toHaveBeenCalledWith('follow')
        })

        it('exec 1 sync post hook', () => {
            new User().follow()

            expect(postMock).toHaveBeenCalledTimes(1)
        })

        it('exec 1 sync post hook event method resolved', async () => {
            await new User().follow()

            expect(postMock).toHaveBeenCalledTimes(1)
        })
    })
})
