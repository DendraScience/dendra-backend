/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const FeathersAuthMixin = require('../../mixins/feathers-auth')
const QueueServiceMixin = require('moleculer-bull')
const { defaultsDeep } = require('lodash')
const { transformQuery } = require('../../lib/query')

module.exports = {
  name: 'station-status',

  mixins: [
    FeathersAuthMixin,
    QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)
  ],

  /**
   * Settings
   */
  settings: {
    feathers: {
      auth: {
        email: process.env.WEB_API_AUTH_EMAIL,
        password: process.env.WEB_API_AUTH_PASS,
        strategy: 'local'
      },
      url: process.env.WEB_API_URL
    }
  },

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
    'monitor.created': {
      strategy: 'RoundRobin',
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string',
            spec: {
              type: 'object',
              props: {
                method: { type: 'enum', values: ['dp'] },
                options: 'object',
                schedule: {
                  type: 'object',
                  default: {
                    // TODO: Bump default
                    every: 60000
                  }
                }
              }
            },
            spec_type: { type: 'equal', value: 'station/status', strict: true }
          }
        }
      },
      async handler(ctx) {
        const monitor = ctx.params.result
        const monitorId = monitor._id
        const pre = {
          queued_at: new Date()
        }

        await ctx.call('monitors.patch', {
          id: monitorId,
          data: { $set: { result_pre: pre, state: 'queued' } }
        })

        // TODO: Deal with deletion
        this.createJob(
          `${this.name}`,
          {
            monitorId,
            meta: ctx.meta,
            method: monitor.spec.method
          },
          {
            jobId: `monitor-${monitorId}`,
            repeat: monitor.spec.schedule
          }
        )
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async dp(_, { monitorId, meta }) {
      /*
        Call actions to fetch datapoints, evaluate and notify.
       */
      const startedAt = new Date()
      const monitor = await this.broker.call(
        'monitors.patch',
        {
          id: monitorId,
          data: {
            $set: {
              result: {
                started_at: startedAt
              },
              state: 'started'
            }
          }
        },
        { meta }
      )

      // Get unique array of station ids based on options
      const options = Object.assign({}, monitor.spec.options)
      let ids = options.station_ids || []
      if (options.station_query)
        ids = ids.concat(
          await this.broker.call(
            'stations.findIds',
            {
              query: transformQuery(
                Object.assign(
                  {},
                  options.station_query,
                  monitor.organization_id
                    ? { organization_id: monitor.organization_id }
                    : {}
                )
              )
            },
            { meta }
          )
        )
      ids = Array.from(new Set(ids))

      // Process each station
      for (const id of ids) {
        this.logger.info(`Processing station: ${id}`)
      }

      // const subprocess = this.execFile(
      //   process.execPath,
      //   [
      //     path.resolve(__dirname, '../../scripts', this.name, 'csv.js'),
      //     monitorId
      //   ],
      //   {
      //     childOptions: {
      //       env: {
      //         ...process.env,
      //         WEB_API_ACCESS_TOKEN: meta.accessToken
      //       }
      //     }
      //   }
      // )

      /*
        Wait for the subprocess to finish.
       */
      // try {
      //   const { stdout, stderr } = await subprocess.promise

      //   process.stdout.write(stdout)
      //   process.stderr.write(stderr)
      // } catch (err) {
      //   this.logger.error(`Subprocess ${subprocess.id} returned error.`)

      //   process.stdout.write(err.stdout)
      //   process.stderr.write(err.stderr)

      //   return this.patchPostError({ monitorId, err, meta, startedAt })
      // }

      const finishedAt = new Date()
      return this.broker.call(
        'monitors.patch',
        {
          id: monitorId,
          data: {
            $set: {
              result_post: {
                duration: finishedAt - startedAt,
                finished_at: finishedAt
              },
              state: 'completed'
            }
          }
        },
        { meta }
      )
    },

    patchPostError({ monitorId, err, meta, startedAt }) {
      const finishedAt = new Date()
      return this.broker.call(
        'monitors.patch',
        {
          id: monitorId,
          data: {
            $set: {
              result_post: {
                duration: finishedAt - startedAt,
                error: err.message,
                finished_at: finishedAt
              },
              state: 'error'
            }
          }
        },
        { meta }
      )
    }
  },

  /**
   * QueueService
   */
  queues: {
    'station-status': {
      concurrency: 1,
      async process({ id, data }) {
        // Switch to the service account
        const { accessToken } = await this.getAuthUser()

        switch (data.method) {
          case 'dp':
            return this.dp(id, defaultsDeep({ meta: { accessToken } }, data))

          default:
            throw new Error(`Unknown job method '${data.method}'.`)
        }
      }
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
