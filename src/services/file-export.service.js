/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const CallQueueMixin = require('../mixins/call-queue')
const ChildProcessMixin = require('../mixins/child-process')

module.exports = {
  name: 'file-export',

  mixins: [CallQueueMixin, ChildProcessMixin],

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
  // actions: {},

  /**
   * Events
   */
  events: {
    'download.created': {
      strategy: 'Shard',
      strategyOptions: {
        shardKey: '_id'
      },
      // TODO: Add datastream_ids to params schema
      params: {
        _id: 'string',
        spec_type: 'string',
        spec: {
          type: 'object',
          props: {
            method: {
              type: 'enum',
              values: ['csvStream']
            },
            options: 'object'
          }
        }
      },
      async handler(ctx) {
        const options = {
          ...ctx.params.spec.options,
          bucket_name: this.name,
          object_name: await ctx.call('moniker.getObjectName', ctx.params)
        }
        if (!options.column_names)
          options.column_names = await ctx.call('moniker.getDatastreamNames', {
            ids: options.datastream_ids
          })

        if (ctx.params.spec_type === 'file/export')
          this.queueMethod(ctx.params.spec.method, [ctx.meta, options])
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async csvStream(id, meta, options) {
      const subprocess = this.execFile(
        process.execPath,
        [
          path.resolve(__dirname, '../scripts', this.name, 'csvStream.js'),
          JSON.stringify(options)
        ],
        {
          childOptions: {
            env: {
              ...process.env,
              WEB_API_ACCESS_TOKEN: meta.accessToken
            }
          }
        }
      )

      await subprocess.promise
        .then(({ stdout, stderr }) => {
          process.stdout.write(stdout)
          process.stderr.write(stderr)
        })
        .catch(err => {
          this.logger.error(`Subprocess returned error: ${err.message}`)
        })
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
