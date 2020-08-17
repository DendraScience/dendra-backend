/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const fs = require('fs')

module.exports = {
  name: 'storage-local',

  /**
   * Settings
   */
  settings: {},

  /**
   * Dependencies
   */
  dependencies: [],

  /**
   * Actions
   */
  actions: {
    get: {
      params: {
        filename: 'string'
      },
      handler(ctx) {
        return fs.createReadStream(`/Users/ssmith/Work/${ctx.params.filename}`)
      }
    },

    save(ctx) {
      const s = fs.createWriteStream(`/Users/ssmith/Work/${ctx.meta.filename}`)
      ctx.params.pipe(s)
    }
  },

  /**
   * Events
   */
  events: {},

  /**
   * Methods
   */
  methods: {},

  /**
   * Service created lifecycle event handler
   */
  created() {},

  /**
   * Service started lifecycle event handler
   */
  async started() {},

  /**
   * Service stopped lifecycle event handler
   */
  async stopped() {}
}
