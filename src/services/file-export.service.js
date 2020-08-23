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
                method: { type: 'enum', values: ['csvStream'] },
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
        const options = {
          ...result.spec.options,
          bucket_name: this.name,
          object_name: await ctx.call('moniker.getObjectName', result)
        }
        if (!options.column_names)
          options.column_names = await ctx.call('moniker.getDatastreamNames', {
            format: options.column_name_format,
            ids: options.datastream_ids
          })

        this.queueMethod(result.spec.method, [ctx.meta, result._id, options])
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async csvStream(_, meta, id, options) {
      /*
        Run a subprocess to fetch and stream datapoints to a Minio object.
       */
      const startedAt = new Date()
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

      await this.broker.call(
        'downloads.patch',
        {
          id,
          data: {
            $set: {
              result: {
                status_info: {
                  started_at: startedAt,
                  state: 'started',
                  subprocess_id: subprocess.id
                }
              }
            }
          }
        },
        { meta }
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

        this.broker.call(
          'downloads.patch',
          {
            id,
            data: {
              $set: {
                result: {
                  status_info: {
                    started_at: startedAt,
                    state: 'subprocess-error',
                    subprocess_id: subprocess.id
                  }
                }
              }
            }
          },
          { meta }
        )

        return
      }

      /*
        Generate a presigned URL for downloading the object from Minio.
       */
      try {
        const { bucket_name: bucketName, object_name: objectName } = options
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

        await this.broker.call(
          'downloads.patch',
          {
            id,
            data: {
              $set: {
                result: {
                  bucket_name: bucketName,
                  object_name: objectName,
                  object_stat: objectStat,
                  presigned_get: {
                    expires_date: expiresDate,
                    expiry: objectExpiry,
                    request_date: requestDate,
                    url: presignedUrl
                  },
                  status_info: {
                    duration: requestDate - startedAt,
                    finished_at: requestDate,
                    started_at: startedAt,
                    state: 'ready',
                    subprocess_id: subprocess.id
                  }
                }
              }
            }
          },
          { meta }
        )
      } catch (err) {
        this.broker.call(
          'downloads.patch',
          {
            id,
            data: {
              $set: {
                result: {
                  status_info: {
                    started_at: startedAt,
                    state: 'presign-error',
                    subprocess_id: subprocess.id
                  }
                }
              }
            }
          },
          { meta }
        )
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
