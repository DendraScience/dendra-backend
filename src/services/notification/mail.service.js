/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const MailMixin = require('moleculer-mail')
const nodemailer = require('nodemailer')

module.exports = {
  mixins: [MailMixin],

  /**
   * Settings
   */
  settings: {
    $secureSettings: ['transport.auth'],

    defaults: {
      replyTo:
        process.env.MAIL_REPLY_TO ||
        '"Dendra Science" <metahuman@dendra.science>'
    },
    from:
      process.env.MAIL_FROM || '"Dendra Bot" <notifications@dendra.science>',
    transport: {
      host: process.env.MAIL_HOST,
      port: process.env.MAIL_PORT | 0,
      secure: process.env.MAIL_SECURE === 'true',
      auth: {
        user: process.env.MAIL_AUTH_USER,
        pass: process.env.MAIL_AUTH_PASS
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    createTransport() {
      return nodemailer.createTransport(
        this.settings.transport,
        this.settings.defaults
      )
    }
  }
}
