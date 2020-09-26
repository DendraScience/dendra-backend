/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const CallQueueMixin = require('../mixins/call-queue')
const ChildProcessMixin = require('../mixins/child-process')
const { merge } = require('lodash')

module.exports = {
  name: 'file-export',

  mixins: [CallQueueMixin, ChildProcessMixin],

  /**
   * Settings
   */
  settings: {
    objectExpiry: process.env.FILE_EXPORT_OBJECT_EXPIRY | 0 || 86400
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
    'downloads.created': {
      strategy: 'Shard',
      strategyOptions: {
        shardKey: '_id'
      },
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string',
            spec: {
              type: 'object',
              props: {
                method: { type: 'enum', values: ['csv'] },
                options: 'object'
              }
            },
            spec_type: { type: 'equal', value: 'file/export', strict: true },
            storage: {
              type: 'object',
              props: {
                method: { type: 'equal', value: 'minio', strict: true }
              }
            }
          }
        }
      },
      async handler(ctx) {
        const { result } = ctx.params
        const bucketName = this.name
        const objectName = await ctx.call('moniker.getObjectName', result)
        const model = merge(
          {
            spec: {
              options: {}
            },
            storage: {
              options: {}
            }
          },
          result,
          {
            result: {
              bucket_name: bucketName,
              object_name: objectName
            }
          }
        )

        // Get unique array of datastream ids based on options
        let ids = model.spec.options.datastream_ids || []
        if (model.spec.options.datastream_query)
          ids = ids.concat(
            await ctx.call('datastreams.findIds', {
              query: model.spec.options.datastream_query
            })
          )
        ids = Array.from(new Set(ids))
        model.spec.options.datastream_ids = ids

        // Get column names
        if (!model.spec.options.column_names)
          model.spec.options.column_names = await ctx.call(
            'moniker.getDatastreamNames',
            {
              format: model.spec.options.column_name_format,
              ids
            }
          )

        this.queueMethod(result.spec.method, [ctx.meta, model])
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async csv(_, meta, model) {
      /*
        Run a subprocess to fetch and stream datapoints to a Minio object.
       */
      const startedAt = new Date()
      model.result.started_at = startedAt
      model.result.state = 'started'
      await this.broker.call(
        'downloads.patch',
        {
          id: model._id,
          data: { $set: { result: model.result } }
        },
        { meta }
      )

      const subprocess = this.execFile(
        process.execPath,
        [
          path.resolve(__dirname, '../scripts', this.name, 'csv.js'),
          JSON.stringify(model)
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

        const download = await this.broker.call('downloads.get', {
          id: model._id
        })
        const finishedAt = new Date()
        download.result.duration = finishedAt - startedAt
        download.result.error = err.message
        download.result.finished_at = finishedAt
        download.result.state = 'error-subprocess'
        download.result.subprocess_id = subprocess.id
        await this.broker.call(
          'downloads.patch',
          {
            id: model._id,
            data: { $set: { result: download.result } }
          },
          { meta }
        )

        return
      }

      /*
        Generate a presigned URL for downloading the object from Minio.
       */
      const download = await this.broker.call(
        'downloads.get',
        {
          id: model._id
        },
        { meta }
      )

      try {
        const {
          bucket_name: bucketName,
          object_name: objectName
        } = download.result
        const { objectExpiry } = this.settings
        const objectStat = await this.broker.call('minio.statObject', {
          bucketName,
          objectName
        })
        const requestDate = new Date()
        const expiresDate = new Date(
          requestDate.getTime() + objectExpiry * 1000
        )
        const presignedUrl = await this.broker.call(
          'minio.presignedGetObject',
          {
            bucketName,
            objectName,
            expires: objectExpiry,
            reqParams: {},
            requestDate: requestDate.toISOString()
          }
        )

        download.result.object_stat = objectStat
        download.result.presigned_get = {
          expires_date: expiresDate,
          expiry: objectExpiry,
          request_date: requestDate,
          url: presignedUrl
        }
        download.result.duration = requestDate - startedAt
        download.result.finished_at = requestDate
        download.result.state = 'finished'
      } catch (err) {
        const finishedAt = new Date()
        download.result.duration = finishedAt - startedAt
        download.result.error = err.message
        download.result.finished_at = finishedAt
        download.result.state = 'error-presigned'
      }

      download.result.subprocess_id = subprocess.id
      await this.broker.call(
        'downloads.patch',
        {
          id: model._id,
          data: { $set: { result: download.result } }
        },
        { meta }
      )
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
