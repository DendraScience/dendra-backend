/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const QueueServiceMixin = require('moleculer-bull')

module.exports = {
  name: 'notification',

  mixins: [QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)]

  /**
   * Settings
   */
  // settings: {},

  /**
   * Actions
   */
  // actions: {},

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  // methods: {},

  /**
   * QueueService
   */
  // queues: {
  //   'notification.email': {
  //     concurrency: 1,
  //     async process() {}
  //   },

  //   'notification.slack': {
  //     concurrency: 1,
  //     async process() {}
  //   }
  // }
}
