/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const ChildProcessMixin = require('../../mixins/child-process')
const QueueServiceMixin = require('moleculer-bull')
const { unescapeQuery } = require('../../lib/query')
const downloadFinishedNotification = require('../../notifications/download-finished')

module.exports = {
  name: 'file-export',

  mixins: [
    ChildProcessMixin,
    QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)
  ],

  /**
   * Settings
   */
  settings: {
    objectExpiry: process.env.FILE_EXPORT_OBJECT_EXPIRY | 0 || 86400
  },

  /**
   * Events
   */
  events: {
    'downloads.created': {
      strategy: 'RoundRobin',
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
        /*
          Pre-processing:
          - Prepare options and save to Minio object.
          - Patch pre result.
          - Queue job.
         */
        const download = ctx.params.result
        const downloadId = download._id
        const jobId = `download-${downloadId}`
        const bucketName = this.name
        const objectName = await ctx.call('moniker.getObjectName', download)
        const pre = {
          bucket_name: bucketName,
          object_name: objectName,
          job_id: jobId,
          queued_at: new Date()
        }
        const options = Object.assign(
          {
            concurrency: 2,
            limit: 2016,
            max_retry_count: 3,
            max_retry_delay: 3000
          },
          download.spec.options
        )

        // Get unique array of datastream ids based on options
        let ids = options.datastream_ids || []
        if (options.datastream_query)
          ids = ids.concat(
            await ctx.call('datastreams.findIds', {
              query: unescapeQuery(options.datastream_query)
            })
          )
        ids = Array.from(new Set(ids))
        options.datastream_ids = ids

        // Get column names
        if (!options.column_names)
          options.column_names = await ctx.call('moniker.getDatastreamNames', {
            format: options.column_name_format,
            ids
          })

        // Put bulky options in object store
        await ctx.call('minio.putObject', JSON.stringify(options), {
          meta: {
            bucketName,
            objectName: `${objectName}.json`
          }
        })

        await ctx.call('downloads.patch', {
          id: downloadId,
          data: { $set: { result_pre: pre, state: 'queued' } }
        })

        await this.createJob(
          `${this.name}`,
          {
            downloadId,
            meta: ctx.meta,
            method: download.spec.method
          },
          {
            jobId,
            removeOnComplete: true,
            removeOnFail: true
          }
        )
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async csv(_, { downloadId, meta }) {
      /*
        Run a subprocess to fetch and stream datapoints to a Minio object.
       */
      const startedAt = new Date()
      const download = await this.broker.call(
        'downloads.patch',
        {
          id: downloadId,
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

      const subprocess = this.spawn(
        process.execPath,
        [
          path.resolve(__dirname, '../../scripts', this.name, 'csv.js'),
          downloadId
        ],
        {
          childOptions: {
            env: {
              ...process.env,
              WEB_API_ACCESS_TOKEN: meta.accessToken
            },
            stdio: 'inherit'
          }
        }
      )

      /*
        Wait for the subprocess to finish.
       */
      try {
        await subprocess.promise
      } catch (err) {
        this.logger.error(`Subprocess ${subprocess.id} returned error.`)
        return this.patchPostError({ downloadId, err, meta, startedAt })
      }

      /*
        Post-processing:
        - Generate a presigned URL for downloading the object from Minio.
        - Patch post result.
       */
      try {
        const { bucket_name: bucketName, object_name: objectName } =
          download.result_pre
        const { objectExpiry } = this.settings
        const objectStat = await this.broker.call('minio.statObject', {
          bucketName,
          objectName
        })
        if (objectStat.versionId === null) delete objectStat.versionId
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
            reqParams: {
              'response-content-encoding': 'gzip',
              'response-content-type': 'text/csv'
            },
            requestDate: requestDate.toISOString()
          }
        )
        const finishedAt = new Date()

        const newDownload = await this.broker.call(
          'downloads.patch',
          {
            id: downloadId,
            data: {
              $set: {
                result_post: {
                  object_stat: objectStat,
                  presigned_get_info: {
                    expires_date: expiresDate,
                    expiry: objectExpiry,
                    request_date: requestDate,
                    url: presignedUrl
                  },
                  duration: finishedAt - startedAt,
                  finished_at: finishedAt
                },
                state: 'completed'
              }
            }
          },
          { meta }
        )

        if (
          newDownload.spec &&
          newDownload.spec.notify &&
          newDownload.spec.notify.length
        ) {
          await this.broker.call(
            'notification.send',
            {
              data: downloadFinishedNotification({ download: newDownload }),
              urls: newDownload.spec.notify
            },
            { meta }
          )
        }
      } catch (err) {
        return this.patchPostError({ downloadId, err, meta, startedAt })
      }
    },

    patchPostError({ downloadId, err, meta, startedAt }) {
      const finishedAt = new Date()
      return this.broker.call(
        'downloads.patch',
        {
          id: downloadId,
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
    'file-export': {
      concurrency: 1,
      async process({ id, data }) {
        switch (data.method) {
          case 'csv':
            return this.csv(id, data)

          default:
            throw new Error(`Unknown job method '${data.method}'.`)
        }
      }
    }
  }
}
