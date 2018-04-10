import { DocumentClient } from 'aws-sdk/clients/dynamodb'

export const { createSet } = DocumentClient.prototype

export const expression = (key: string) => <T>(op?: string, op2?: string) => (val?: T) => {
    const data: ExpressionData = {
        exprs: [],
        names: {},
        values: {},
    }

    // Convert a JavaScript value to its equivalent DynamoDB AttributeValue type
    if (val) {
        if (val instanceof Set) {
            val = createSet(Array.from(val)) as any
        }
    }

    // if args are user.pets[3].age = 9
    const keys = key.split('.')
    // then sign is #user.#pets[3].#age
    const sign = keys.map(k => `#${k}`).join('.')
    // and colon is :user_pets_3_age
    const colon = ':' + key.replace(/\.|\[/g, '_').replace(/\]/g, '')
    // names become { '#user': 'user', '#pets': 'pets', '#age': 'age' }
    keys.forEach(k => {
        const p = k.replace(/\[\d+\]/g, '')
        data.names[`#${p}`] = p
    })

    const e = (expr: string) => data.exprs.push(expr)
    const v = (name: string, value) => data.values[`${name}`] = value

    switch (op) {
        case '=':
        case '>':
        case '>=':
        case '<':
        case '<=':
        case '<>':
            if (op2) {
                // size(n) = 3
                e(`${op2}(${sign}) ${op} ${colon}_${op2}`)
                v(`${colon}_${op2}`, val)
            } else {
                // i > 1
                e(`${sign} ${op} ${colon}`)
                v(`${colon}`, val)
            }
            break
        // i BETWEEN 1 AND 3
        case 'BETWEEN':
            e(`${sign} ${op} ${colon}_lower AND ${colon}_upper`)
            v(`${colon}_lower`, val[0])
            v(`${colon}_upper`, val[1])
            break

        // i IN (1, 2, 3)
        case 'IN':
            let keys = []
            let arr = val as any
            arr.forEach((a, i) => {
                keys.push(`${colon}_index_${i}`)
                v(`${colon}_index_${i}`, a)
            })
            e(`${sign} ${op} (${keys.join(', ')})`)
            break

        // attribute_exists(i)
        case 'attribute_exists':
        case 'attribute_not_exists':
            e(`${op}(${sign})`)
            break

        // attribute_type(i, 'N')
        case 'attribute_type':
        case 'begins_with':
        case 'contains':
            e(`${op}(${sign}, ${colon}_${op})`)
            v(`${colon}_${op}`, val)
            break

        // SET i = i + 1
        case '+':
            e(`${sign} = ${sign} + ${colon}_increase`)
            v(`${colon}_increase`, val)
            break

        // SET i = i - 1
        case '-':
            e(`${sign} = ${sign} - ${colon}_decrease`)
            v(`${colon}_decrease`, val)
            break

        // SET l = list_append(l, :v)
        case 'list_append':
        case 'if_not_exists':
            if (op2 === 'prepend') {
                // SET l = list_append(:v, l)
                e(`${sign} = ${op}(${colon}_${op}_prepend, ${sign})`)
                v(`${colon}_${op}_prepend`, val)
            } else {
                e(`${sign} = ${op}(${sign}, ${colon}_${op})`)
                v(`${colon}_${op}`, val)
            }
            break

        case 'REMOVE':
            if (val) {
                // REMOVE p[1], p[2]
                let indexes = Array.isArray(val) ? <T[]>val : [val]
                e(indexes.map(idx => `${sign}[${idx}]`).join(', '))
            } else {
                // REMOVE p
                e(sign)
            }
            break

        // ADD p 1
        case 'ADD':
            e(`${sign} ${colon}_add`)
            v(`${colon}_add`, val)
            break

        // DELETE p :l
        case 'DELETE':
            e(`${sign} ${colon}_delete`)
            v(`${colon}_delete`, val)
            break

        // only generate names. e.g. projection expression
        default:
            e(sign)
            break
    }

    return data
}

/* TYPES */

export type ExpressionData = {
    exprs: string[]
    names: {
        [name: string]: string
    }
    values: {
        [name: string]: string
    }
}

export type ExpressionLogic = 'AND' | 'OR' | 'NOT'
