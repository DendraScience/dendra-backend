/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const CallQueueMixin = require('../../mixins/call-queue')
const ChildProcessMixin = require('../../mixins/child-process')
const FeathersAuthMixin = require('../../mixins/feathers-auth')

module.exports = {
  name: 'datapoints-config',

  mixins: [CallQueueMixin, ChildProcessMixin, FeathersAuthMixin],

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
   * Events
   */
  events: {
    'annotations.*': {
      // BUG: Shard strategy is broken until this is resolved https://github.com/moleculerjs/moleculer/issues/1072
      // strategy: 'Shard',
      // strategyOptions: {
      //   shardKey: 'result._id'
      // },
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string'
          }
        }
      },
      async handler(ctx) {
        if (
          ![
            'annotations.created',
            'annotations.patched',
            'annotations.removed',
            'annotations.updated'
          ].includes(ctx.eventName)
        ) {
          this.logger.info(
            `Service ${this.name} ignoring event '${ctx.eventName}'.`
          )
          return
        }

        // Switch to the service account
        const { accessToken } = await this.getAuthUser()

        /*
          Collect station and datastream ids impacted.
         */
        const annotation = ctx.params.result
        const annotationBefore = ctx.params.before
        let affectedStationIds = []
        let datastreamIds = []
        let stationIds = []

        this.logger.info(`Processing annotation ${annotation._id}.`)

        // Gather and unique selected ids
        if (annotation) {
          if (annotation.datastream_ids)
            datastreamIds.push(...annotation.datastream_ids)
          if (annotation.station_ids) stationIds.push(...annotation.station_ids)
        }
        if (annotationBefore) {
          if (annotationBefore.datastream_ids)
            datastreamIds.push(...annotationBefore.datastream_ids)
          if (annotationBefore.station_ids)
            stationIds.push(...annotationBefore.station_ids)
        }
        datastreamIds = [...new Set(datastreamIds)]
        stationIds = [...new Set(stationIds)]

        // Gather affected station ids (used to patch annotation)
        affectedStationIds = [...stationIds]
        for (const datastreamId of datastreamIds) {
          const datastream = await ctx.call(
            'datastreams.findOne',
            {
              query: {
                _id: datastreamId,
                $select: ['_id', 'station_id']
              }
            },
            {
              meta: { accessToken }
            }
          )
          if (datastream && datastream.station_id)
            affectedStationIds.push(datastream.station_id)
        }
        affectedStationIds = [...new Set(affectedStationIds)]

        // Gather the datastream ids for each selected station
        this.logger.info(
          `Finding datastreams for (${stationIds.length}) selected stations.`
        )
        for (const stationId of stationIds) {
          const ids = await ctx.call(
            'datastreams.findIds',
            {
              query: {
                source_type: 'sensor',
                station_id: stationId
              }
            },
            {
              meta: { accessToken }
            }
          )
          datastreamIds = [...new Set([...datastreamIds, ...ids])]
        }

        // Trigger config rebuild for resolved datastream ids
        this.logger.info(
          `Triggering config rebuild for (${datastreamIds.length}) datastreams.`
        )
        for (const datastreamId of datastreamIds) {
          this.broker.emit('datastreams.annotated', {
            result: {
              _id: datastreamId
            }
          })
        }

        // Patch affected station ids on the annotation
        if (annotation) {
          this.logger.info(
            `Patching annotation ${annotation._id} with (${affectedStationIds.length}) affected stations.`
          )
          await ctx.call(
            'annotations.patch',
            {
              id: annotation._id,
              data: affectedStationIds.length
                ? {
                    $set: {
                      affected_station_ids: affectedStationIds
                    }
                  }
                : {
                    $unset: {
                      affected_station_ids: ''
                    }
                  }
            },
            {
              meta: { accessToken }
            }
          )
        }
      }
    },

    'datastreams.*': {
      // BUG: Shard strategy is broken until this is resolved https://github.com/moleculerjs/moleculer/issues/1072
      // strategy: 'Shard',
      // strategyOptions: {
      //   shardKey: 'result._id'
      // },
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string'
          }
        }
      },
      async handler(ctx) {
        // TODO: Add keys for patched $set/$unset to event and check for them
        if (
          ![
            'datastreams.annotated',
            'datastreams.created',
            'datastreams.patched',
            'datastreams.updated'
          ].includes(ctx.eventName)
        ) {
          this.logger.info(
            `Service ${this.name} ignoring event '${ctx.eventName}'.`
          )
          return
        }

        const datastream = ctx.params.result

        this.logger.info(`Queuing build for datastream ${datastream._id}.`)

        return this.queueMethod('build', [datastream._id])
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async build(_, datastreamId) {
      // Switch to the service account
      const { accessToken } = await this.getAuthUser()

      this.logger.info(`Running build script for datastream ${datastreamId}.`)

      /*
        Run a subprocess to build the datapoints config.
       */
      const subprocess = this.execFile(
        process.execPath,
        [
          path.resolve(__dirname, '../../scripts', this.name, 'build.js'),
          datastreamId
        ],
        {
          childOptions: {
            env: {
              ...process.env,
              WEB_API_ACCESS_TOKEN: accessToken
            }
          }
        }
      )

      /*
        Wait for the subprocess to finish.
       */
      try {
        const { stdout, stderr } = await subprocess.promise

        process.stdout.write(stdout)
        process.stderr.write(stderr)
      } catch (err) {
        this.logger.error(`Subprocess ${subprocess.id} returned error.`)

        process.stdout.write(err.stdout)
        process.stderr.write(err.stderr)
      }
    }
  }
}
