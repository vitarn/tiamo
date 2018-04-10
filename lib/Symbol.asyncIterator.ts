/**
 * Symbol asyncIterator shim
 * @see https://www.typescriptlang.org/docs/handbook/release-notes/typescript-2-3.html#the-for-await-of-statement
 */
(Symbol as any).asyncIterator = (Symbol as any).asyncIterator || Symbol.for('Symbol.asyncIterator')
