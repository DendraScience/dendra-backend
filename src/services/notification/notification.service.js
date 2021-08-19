/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const QueueServiceMixin = require('moleculer-bull')
const { IncomingWebhook } = require('@slack/webhook')

const sendParamsSchema = {
  data: {
    type: 'object',
    props: {
      md: 'string',
      short_text: 'string',
      subject: 'string',
      text: 'string'
    }
  },
  urls: {
    type: 'array',
    items: [
      { type: 'string' },
      {
        type: 'object',
        props: {
          params: 'string',
          pathname: 'string',
          protocol: { type: 'string', default: 'dendra:' }
        }
      }
    ]
  }
}

module.exports = {
  name: 'notification',

  mixins: [QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)],

  /**
   * Actions
   */
  actions: {
    send: {
      params: sendParamsSchema,
      handler(ctx) {
        return Promise.all([
          this.actions.mail(ctx.params, { parentCtx: ctx }),
          this.actions.slack(ctx.params, { parentCtx: ctx })
          // this.actions.sms(ctx.params, { parentCtx: ctx })
        ])
      }
    },

    mail: {
      params: sendParamsSchema,
      async handler(ctx) {
        const { data } = ctx.params
        const parsedUrls = this.parseAndFilterUrls(ctx.params.urls, 'mail')
        const bcc = []

        this.logger.info(
          `Sending notifications to (${parsedUrls.length}) mail recipients.`
        )

        for (const parsedUrl of parsedUrls) {
          const { id, service } = parsedUrl.params

          if (!id) {
            this.logger.warn(`Missing id param in mail url.`)
            continue
          }
          if (!service) {
            this.logger.warn(`Missing service param in mail url.`)
            continue
          }

          let resource
          try {
            resource = await ctx.call(`${service}.get`, { id })
          } catch (err) {
            this.logger.error(
              `Get '${service}' id '${id}' failed: ${err.message}`
            )
            continue
          }

          const { email } = resource
          if (typeof email !== 'string') {
            this.logger.warn(`Invalid email on '${service}' id '${id}'.`)
            continue
          }

          bcc.push(email)
        }

        if (bcc.length)
          this.createJob(
            `${this.name}.mail`,
            {
              bcc,
              subject: data.subject,
              text: data.text || data.short_text
            },
            {
              removeOnComplete: true,
              removeOnFail: true
            }
          )
      }
    },

    slack: {
      params: sendParamsSchema,
      handler(ctx) {
        const { data } = ctx.params
        const parsedUrls = this.parseAndFilterUrls(ctx.params.urls, 'slack')

        this.logger.info(
          `Sending notifications to (${parsedUrls.length}) Slack webhooks.`
        )

        for (const parsedUrl of parsedUrls) {
          const { webhook } = parsedUrl.params

          if (!webhook) {
            this.logger.warn(`Missing webhook param in Slack url.`)
            continue
          }
          if (!this.slackWebhooks[webhook]) {
            const url = process.env[`${webhook}_SLACK_WEBHOOK_URL`]
            if (!url) {
              this.logger.warn(`Slack webhook '${webhook}' not configured.`)
              continue
            }

            // Set up and cache an IncomingWebhook
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
    }
  },

  /**
   * Methods
   */
  methods: {
    parseAndFilterUrls(urls, pathname) {
      return urls
        .map(url => (typeof url === 'object' ? url : this.parseUrl(url)))
        .filter(
          urlObj =>
            urlObj.protocol === 'dendra:' && urlObj.pathname === pathname
        )
    },

    parseUrl(urlStr) {
      const urlObj = new URL(urlStr)
      return {
        params: Object.fromEntries(urlObj.searchParams.entries()),
        pathname: urlObj.pathname,
        protocol: urlObj.protocol
      }
    }
  },

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
    'notification.mail': {
      concurrency: 1,
      async process({ data }) {
        return this.broker.call('mail.send', data)
      }
    },

    'notification.slack': {
      concurrency: 1,
      process({ data: { text, webhook } }) {
        return this.slackWebhooks[webhook].send({ text })
      }
    }
  }
}
