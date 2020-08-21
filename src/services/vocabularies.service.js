/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const FeathersMixin = require('../mixins/feathers')

module.exports = {
  name: 'vocabularies',

  mixins: [FeathersMixin],

  settings: {
    url: process.env.WEB_API_URL
  },

  /**
   * Actions
   */
  actions: {
    async getUnit(ctx) {
      const { id = 'dt-unit' } = ctx.params
      const vocabulary = await this.actions.get({ id }, { parentCtx: ctx })

      if (vocabulary.vocabulary_type !== 'unit')
        throw new Error(`Not a unit vocabulary type '${id}'.`)

      return vocabulary
    },

    async getUnitTermsByTag(ctx) {
      const vocabulary = await this.actions.getUnit(ctx.params, {
        parentCtx: ctx
      })

      return vocabulary.terms.reduce((obj, term) => {
        obj[`${vocabulary.scheme_id}_${vocabulary.label}_${term.label}`] = term
        return obj
      }, {})
    }
  }
}
