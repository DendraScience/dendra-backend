/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

module.exports = {
  name: 'webhooks',

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
}
