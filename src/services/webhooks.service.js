/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

module.exports = {
  name: 'webhooks',

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
    frontend: {
      rest: {
        method: 'POST'
      },
      async handler(ctx) {
        const { event } = ctx.meta
        delete ctx.meta.event

        this.logger.info(`Received frontend webhook event: ${event}`)

        ctx.emit(event, ctx.params)
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
  // methods: {},

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
