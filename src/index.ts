import { DocumentClient } from 'aws-sdk/clients/dynamodb'

export const { createSet } = DocumentClient.prototype
export { Model } from './model'
export * from './decorator'
export { Schema, SchemaOptions, SchemaProperties } from 'tdv'
