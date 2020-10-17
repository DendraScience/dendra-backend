/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const MinioMixin = require('moleculer-minio')

module.exports = {
  // name: '',

  mixins: [MinioMixin],

  /**
   * Settings
   */
  settings: {
    $secureSettings: ['accessKey', 'secretKey'],

    endPoint: process.env.MINIO_END_POINT,
    port: process.env.MINIO_PORT | 0,
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY,
    useSSL: process.env.MINIO_USE_SSL === 'true'
  },

  /**
   * Actions
   */
  actions: {
    listObjectsV2WithMetadata: {
      params: {
        bucketName: { type: 'string' },
        prefix: { type: 'string', optional: true },
        recursive: { type: 'boolean', optional: true },
        startAfter: { type: 'string', optional: true }
      },
      handler(ctx) {
        return this.Promise.resolve(ctx.params).then(
          ({ bucketName, prefix = '', recursive = false, startAfter = '' }) => {
            return new this.Promise((resolve, reject) => {
              try {
                const stream = this.client.extensions.listObjectsV2WithMetadata(
                  bucketName,
                  prefix,
                  recursive,
                  startAfter
                )
                const objects = []
                stream.on('data', el => objects.push(el))
                stream.on('end', () => resolve(objects))
                stream.on('error', reject)
              } catch (e) {
                reject(e)
              }
            })
          }
        )
      }
    }
  }
}
