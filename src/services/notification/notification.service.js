/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const QueueServiceMixin = require('moleculer-bull')
const { IncomingWebhook } = require('@slack/webhook')

module.exports = {
  name: 'notification',

  mixins: [QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)],

  /**
   * Settings
   */
  // settings: {},

  /**
   * Actions
   */
  actions: {
    send: {
      params: {
        data: {
          type: 'object',
          props: {
            md: 'string',
            short_text: 'string',
            subject: 'string',
            text: 'string'
          }
        },
        url: 'string'
      },
      handler(ctx) {
        const toURL = new URL(ctx.params.url)

        if (toURL.protocol !== 'dendra:')
          throw new Error(`Not a valid protocol '${toURL.protocol}'.`)

        const action = this.actions[toURL.pathname]
        if (!action)
          throw new Error(`Not a valid pathname '${toURL.pathname}'.`)

        return action(
          Object.assign({}, Object.fromEntries(toURL.searchParams.entries()), {
            data: ctx.params.data
          }),
          { parentCtx: ctx }
        )
      }
    },

    sendMany: {
      params: {
        data: {
          type: 'object',
          props: {
            md: 'string',
            short_text: 'string',
            subject: 'string',
            text: 'string'
          }
        },
        urls: { type: 'array', items: 'string' }
      },
      async handler(ctx) {
        for (const url of ctx.params.urls) {
          try {
            await this.actions.send(
              { data: ctx.params.data, url },
              { parentCtx: ctx }
            )
          } catch (err) {
            this.logger.warn(`Notification send error: ${err.message}`)
          }
        }
      }
    },

    slack: {
      params: {
        data: {
          type: 'object',
          props: {
            md: 'string',
            short_text: 'string',
            subject: 'string',
            text: 'string'
          }
        },
        webhook: 'string'
      },
      async handler(ctx) {
        const { data, webhook } = ctx.params

        if (!this.slackWebhooks[webhook]) {
          const url = process.env[`${webhook}_SLACK_WEBHOOK_URL`]
          if (!url) throw new Error(`Not a valid webhook '${webhook}'.`)

          this.slackWebhooks[webhook] = new IncomingWebhook(url)
        }

        this.createJob(
          `${this.name}.slack`,
          {
            text: data.md || data.text || data.short_text,
            webhook
          },
          {
            removeOnComplete: true,
            removeOnFail: true
          }
        )
      }
    }
  },

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
  created() {
    this.slackWebhooks = {}
  },

  /**
   * QueueService
   */
  queues: {
    //   'notification.email': {
    //     concurrency: 1,
    //     async process() {}
    //   },

    'notification.slack': {
      concurrency: 1,
      process({ data }) {
        return this.slackWebhooks[data.webhook].send({
          text: data.text
        })
      }
    }
  }
}
