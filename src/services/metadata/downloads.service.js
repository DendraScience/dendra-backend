/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const FeathersMixin = require('../../mixins/feathers')

module.exports = {
  name: 'downloads',

  mixins: [FeathersMixin],

  /**
   * Settings
   */
  settings: {
    url: process.env.WEB_API_URL
  }
}
