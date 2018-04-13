import { expression } from '../src/expression'

describe('expression', () => {
    it('prop = 1', () => {
        let expr = expression('prop')('=')(1)

        expect(expr.exprs).toEqual(['#prop = :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop > 1', () => {
        let expr = expression('prop')('>')(1)

        expect(expr.exprs).toEqual(['#prop > :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop >= 1', () => {
        let expr = expression('prop')('>=')(1)

        expect(expr.exprs).toEqual(['#prop >= :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop < 1', () => {
        let expr = expression('prop')('<')(1)

        expect(expr.exprs).toEqual(['#prop < :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop <= 1', () => {
        let expr = expression('prop')('<=')(1)

        expect(expr.exprs).toEqual(['#prop <= :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop <> 1', () => {
        let expr = expression('prop')('<>')(1)

        expect(expr.exprs).toEqual(['#prop <> :prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop': 1 })
    })

    it('prop.pet.name = abc', () => {
        let expr = expression('prop.pet.name')('=')('abc')

        expect(expr.exprs).toEqual(['#prop.#pet.#name = :prop_pet_name'])
        expect(expr.names).toEqual({ '#prop': 'prop', '#pet': 'pet', '#name': 'name' })
        expect(expr.values).toEqual({ ':prop_pet_name': 'abc' })
    })

    it('prop.pet.name[2] = abc', () => {
        let expr = expression('prop.pet.name[2]')('=')('abc')

        expect(expr.exprs).toEqual(['#prop.#pet.#name[2] = :prop_pet_name_2'])
        expect(expr.names).toEqual({ '#prop': 'prop', '#pet': 'pet', '#name': 'name' })
        expect(expr.values).toEqual({ ':prop_pet_name_2': 'abc' })
    })

    it('prop.pets[2].name = abc', () => {
        let expr = expression('prop.pets[2].name')('=')('abc')

        expect(expr.exprs).toEqual(['#prop.#pets[2].#name = :prop_pets_2_name'])
        expect(expr.names).toEqual({ '#prop': 'prop', '#pets': 'pets', '#name': 'name' })
        expect(expr.values).toEqual({ ':prop_pets_2_name': 'abc' })
    })

    it('prop BETWEEN 1 AND 2', () => {
        let expr = expression('prop')('BETWEEN')([1, 2])

        expect(expr.exprs).toEqual(['#prop BETWEEN :prop_lower AND :prop_upper'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_lower': 1, ':prop_upper': 2 })
    })

    it('prop IN (1, 2)', () => {
        let expr = expression('prop')('IN')([1, 2, 3])

        expect(expr.exprs).toEqual(['#prop IN (:prop_index_0, :prop_index_1, :prop_index_2)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_index_0': 1, ':prop_index_1': 2, ':prop_index_2': 3 })
    })

    it('attribute_exists(prop)', () => {
        let expr = expression('prop')('attribute_exists')()

        expect(expr.exprs).toEqual(['attribute_exists(#prop)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({})
    })

    it('attribute_not_exists(prop)', () => {
        let expr = expression('prop')('attribute_not_exists')()

        expect(expr.exprs).toEqual(['attribute_not_exists(#prop)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({})
    })

    it('attribute_type(prop, N)', () => {
        let expr = expression('prop')('attribute_type')('N')

        expect(expr.exprs).toEqual(['attribute_type(#prop, :prop_attribute_type)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_attribute_type': 'N' })
    })

    it('begins_with(prop, abc)', () => {
        let expr = expression('prop')('begins_with')('abc')

        expect(expr.exprs).toEqual(['begins_with(#prop, :prop_begins_with)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_begins_with': 'abc' })
    })

    it('contains(prop, abc)', () => {
        let expr = expression('prop')('contains')('abc')

        expect(expr.exprs).toEqual(['contains(#prop, :prop_contains)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_contains': 'abc' })
    })

    it('size(prop) = 32', () => {
        let expr = expression('prop')('=', 'size')(32)

        expect(expr.exprs).toEqual(['size(#prop) = :prop_size'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_size': 32 })
    })

    it('SET prop = prop + 1', () => {
        let expr = expression('prop')('+')(1)

        expect(expr.exprs).toEqual(['#prop = #prop + :prop_increase'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_increase': 1 })
    })

    it('SET prop = prop - 1', () => {
        let expr = expression('prop')('-')(1)

        expect(expr.exprs).toEqual(['#prop = #prop - :prop_decrease'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_decrease': 1 })
    })

    it('SET prop = list_append(prop, :list)', () => {
        let expr = expression('prop')('list_append')([1])

        expect(expr.exprs).toEqual(['#prop = list_append(#prop, :prop_list_append)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_list_append': [1] })
    })

    it('SET prop = list_append(:list, prop)', () => {
        let expr = expression('prop')('list_append', 'prepend')([1])

        expect(expr.exprs).toEqual(['#prop = list_append(:prop_list_append_prepend, #prop)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_list_append_prepend': [1] })
    })

    it('SET prop = if_not_exists(1)', () => {
        let expr = expression('prop')('if_not_exists')(1)

        expect(expr.exprs).toEqual(['#prop = if_not_exists(#prop, :prop_if_not_exists)'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_if_not_exists': 1 })
    })

    it('REMOVE prop', () => {
        let expr = expression('prop')('REMOVE')()

        expect(expr.exprs).toEqual(['#prop'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
    })

    it('REMOVE prop[1]', () => {
        let expr = expression('prop')('REMOVE')(1)

        expect(expr.exprs).toEqual(['#prop[1]'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
    })

    it('REMOVE prop[1], prop[2]', () => {
        let expr = expression('prop')('REMOVE')([1, 2])

        expect(expr.exprs).toEqual(['#prop[1], #prop[2]'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
    })

    it('ADD prop 1', () => {
        let expr = expression('prop')('ADD')(1)

        expect(expr.exprs).toEqual(['#prop :prop_add'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_add': 1 })
    })

    it('ADD prop Set', () => {
        let expr = expression('prop')('ADD')(new Set([1]))

        expect(expr.exprs).toEqual(['#prop :prop_add'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_add': { values: [1], type: 'Number', wrapperName: 'Set' } })
    })

    it('DELETE prop :list', () => {
        let expr = expression('prop')('DELETE')(new Set([1, 2]))

        expect(expr.exprs).toEqual(['#prop :prop_delete'])
        expect(expr.names).toEqual({ '#prop': 'prop' })
        expect(expr.values).toEqual({ ':prop_delete': { values: [1, 2], type: 'Number', wrapperName: 'Set' } })
    })

    it('ProjectionExpression', () => {
        let expr = expression('prop.sub[1]')()()

        expect(expr.exprs).toEqual(['#prop.#sub[1]'])
        expect(expr.names).toEqual({ '#prop': 'prop', '#sub': 'sub' })
        expect(expr.values).toEqual({})
    })
})
