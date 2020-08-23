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
    async create(ctx) {
      const { body } = await got(`${this.name}`, {
        headers: this.makeHeaders(ctx.meta),
        json: ctx.params.data,
        method: 'POST',
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return body
    },

    async find(ctx) {
      const { body } = await got(this.name, {
        headers: this.makeHeaders(ctx.meta),
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return Readable.from(body.data)
    },

    async get(ctx) {
      if (!ctx.params.id) throw new Error("id for 'get' can not be undefined")

      const { body } = await got(`${this.name}/${ctx.params.id}`, {
        headers: this.makeHeaders(ctx.meta),
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return body
    },

    async patch(ctx) {
      const { body } = await got(
        ctx.params.id ? `${this.name}/${ctx.params.id}` : `${this.name}`,
        {
          headers: this.makeHeaders(ctx.meta),
          json: ctx.params.data,
          method: 'PATCH',
          prefixUrl: this.settings.url,
          responseType: 'json',
          searchParams: qs.stringify(ctx.params.query)
        }
      )

      return body
    },

    async update(ctx) {
      if (!ctx.params.id)
        throw new Error("id for 'update' can not be undefined")

      const { body } = await got(`${this.name}/${ctx.params.id}`, {
        headers: this.makeHeaders(ctx.meta),
        json: ctx.params.data,
        method: 'PUT',
        prefixUrl: this.settings.url,
        responseType: 'json',
        searchParams: qs.stringify(ctx.params.query)
      })

      return body
    }
  },

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  methods: {
    makeHeaders(meta) {
      const headers = {}
      if (meta.accessToken) headers.Authorization = meta.accessToken

      return headers
    }
  }

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
