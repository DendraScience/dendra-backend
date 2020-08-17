const got = require('got')
const qs = require('qs')
const { Readable } = require('stream')

module.exports = {
  // name: 'service',

  /**
   * Settings
   */
  // settings: {},

  /**
   * Dependencies
   */
  // dependencies: [],

  /**
   * Actions
   */
  actions: {
    find: {
      async handler(ctx) {
        const headers = {}
        if (ctx.meta.accessToken) headers.Authorization = ctx.meta.accessToken

        const { body } = await got(this.name, {
          headers,
          prefixUrl: this.settings.url,
          responseType: 'json',
          searchParams: qs.stringify(ctx.params)
        })

        return Readable.from(body.data)
      }
    }
  }

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  // methods: {}

  /**
   * Service created lifecycle event handler
   */
  // created() {},

  /**
   * Service started lifecycle event handler
   */
  // async started() {},

  /**
   * Service stopped lifecycle event handler
   */
  // async stopped() {}
}
