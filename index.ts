import { DocumentClient } from 'aws-sdk/clients/dynamodb'

export const { createSet } = DocumentClient.prototype
export { Model } from './lib/model'
export * from './lib/decorator'
export { Schema, SchemaOptions, SchemaProperties } from 'tdv'
