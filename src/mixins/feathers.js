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
    async find(ctx) {
      const headers = {}
      if (ctx.meta.accessToken) headers.Authorization = ctx.meta.accessToken

      const { body } = await got(this.name, {
        headers,
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return Readable.from(body.data)
    },

    async get(ctx) {
      if (!ctx.params.id) throw new Error("id for 'get' can not be undefined")

      const headers = {}
      if (ctx.meta.accessToken) headers.Authorization = ctx.meta.accessToken

      const { body } = await got(`${this.name}/${ctx.params.id}`, {
        headers,
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return body
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
