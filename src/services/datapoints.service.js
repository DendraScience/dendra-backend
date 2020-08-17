/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const FeathersMixin = require('../mixins/feathers')

module.exports = {
  name: 'datapoints',

  mixins: [FeathersMixin],

  /**
   * Actions
   */
  actions: {
    // query: {
    //   timeout: 60 * 60 * 1000,
    //   async handler(ctx) {
    //     return Readable.from(query(this, ctx))
    //   }
    // }
  }
}
