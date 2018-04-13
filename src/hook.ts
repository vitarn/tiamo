export class Hook {
    private hooks = {} as {
        [name: string]: Function[]
    }

    /**
     * Append pre action
     *
     * @param name action name
     * @param fn hooker function
     */
    pre(name: string, fn: Function,) {
        return this.add('pre', name, fn)
    }

    /**
     * Append post action
     *
     * @param name action name
     * @param fn hooker function
     */
    post(name: string, fn: Function) {
        return this.add('post', name, fn)
    }

    private add(stage: 'pre' | 'post', name: string, fn: Function) {
        name = `${stage}:${name}`

        const hooks = this.hooks[name] = this.hooks[name] || []

        hooks.push(fn)

        return this
    }

    /**
     * Execute pre actions
     * 
     * If there are asynchronous hooks, return Promise.
     * 
     * @param name action name
     * @param ctx execute context
     */
    execPre(name: string, ctx?) {
        return this.exec('pre', name, ctx)
    }

    /**
     * Execute post actions
     * 
     * If there are asynchronous hooks, return Promise.
     * 
     * @param name action name
     * @param ctx execute context
     */
    execPost(name: string, ctx?) {
        return this.exec('post', name, ctx)
    }

    private exec(stage: 'pre' | 'post', name: string, ctx?) {
        name = `${stage}:${name}`

        const hooks = this.hooks[name]
        if (!hooks) return

        let promise: PromiseLike<any>

        for (let fn of hooks) {
            // if promise exist. just put another hooks in then chains.
            if (promise) {
                promise = promise.then(() => fn.apply(ctx))
            } else {
                // if fn return promise. remember it. otherwise leave undefined.
                promise = this.thenable(fn.apply(ctx))
            }
        }

        return promise
    }

    /**
     * Wrap a function.
     * 
     * Call this wrapper will:
     * 1. Execute pre hooks. If pre return Promise. Waiting it.
     * 2. Apply wrapped function.
     * 3. Execute post hooks but ignore asynchronous result.
     * 4. Return wrapped function result.
     * 
     * @param name action name
     * @param fn wrapped function
     * @param ctx execute context
     */
    wrap<T extends Function>(name: string, fn: T, ctx?) {
        return ((...args) => {
            const promise = this.thenable(this.execPre(name, ctx))

            const result = promise
                ? promise.then(() => fn.apply(ctx, args))
                : fn.apply(ctx, args)
            
            this.execPost(name, ctx)

            return result
        }) as any as T
    }

    /**
     * Check if target is thenable. If true return it. otherwise return undefined.
     */
    private thenable<T extends { then?: Function }>(target: T) {
        return target && typeof target.then === 'function' && target
    }
}
