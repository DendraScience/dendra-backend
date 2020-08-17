/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const MinioMixin = require('moleculer-minio')

module.exports = {
  // name: '',

  mixins: MinioMixin,

  /**
   * Settings
   */
  settings: {
    $secureSettings: ['accessKey', 'secretKey'],

    endPoint: process.env.MINIO_END_POINT,
    port: process.env.MINIO_PORT | 0,
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY,
    useSSL: false
  }

  /**
   * Dependencies
   */
  // dependencies: [],

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
