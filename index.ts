import { DocumentClient } from 'aws-sdk/clients/dynamodb'

export const { createSet } = DocumentClient.prototype
export { Model, ModelProperties } from './lib/model'
export * from './lib/decorator'
