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
      // TODO: Add datastream_ids to event schema
      async handler(ctx) {
        console.log('Event download.created')

        console.log('Metadata:', ctx.meta)

        const names = await ctx.call('moniker.nameDatastreams', {
          ids: ctx.params.spec.options.datastream_ids
        })

        if (ctx.params.spec_type === 'file/export')
          this.queueMethod(ctx.params.spec.method, [
            ctx.meta,
            ctx.params,
            names
          ])
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async csvStream(id, meta, params, names) {
      const { options } = params.spec
      const subprocess = this.execFile(
        process.execPath,
        [
          path.resolve(__dirname, '../scripts', this.name, 'csvStream.js'),
          JSON.stringify({ names, options })
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
