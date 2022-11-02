/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const ChildProcessMixin = require('../../mixins/child-process')
const QueueServiceMixin = require('moleculer-bull')
const uploadFinishedNotification = require('../../notifications/upload-finished')

module.exports = {
  name: 'file-import',

  mixins: [
    ChildProcessMixin,
    QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL)
  ],

  /**
   * Events
   */
  events: {
    'uploads.created': {
      strategy: 'RoundRobin',
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string',
            organization_id: 'string',
            spec: {
              type: 'object',
              props: {
                method: { type: 'enum', values: ['csv'] },
                options: 'object'
              }
            },
            spec_type: { type: 'equal', value: 'file/import', strict: true },
            storage: {
              type: 'object',
              props: {
                method: { type: 'equal', value: 'minio', strict: true },
                options: {
                  type: 'object',
                  props: {
                    object_limit: {
                      type: 'number',
                      integer: true,
                      positive: true,
                      default: 1
                    },
                    object_prefix: 'string'
                  }
                }
              }
            }
          }
        }
      },
      async handler(ctx) {
        /*
          Pre-processing:
          - Patch pre result.
          - Queue job.
         */
        const upload = ctx.params.result
        const uploadId = upload._id
        const jobId = `upload-${uploadId}`
        const bucketName = this.name
        const organization = await ctx.call('organizations.get', {
          id: upload.organization_id
        })
        const pre = {
          bucket_name: bucketName,
          object_list: Array.prototype.slice.call(
            await ctx.call('minio.listObjectsV2WithMetadata', {
              bucketName,
              prefix: upload.storage.options.object_prefix
            }),
            0,
            upload.storage.options.object_limit
          ),
          job_id: jobId,
          org_slug: organization.slug,
          pub_to_subject: await ctx.call('moniker.getWorkerSubject', {
            action: 'import',
            org_slug: organization.slug,
            suffix: ['file'],
            type: 'out'
          }),
          queued_at: new Date()
        }

        await ctx.call('uploads.patch', {
          id: uploadId,
          data: { $set: { result_pre: pre, state: 'queued' } }
        })

        await this.createJob(
          `${this.name}`,
          {
            uploadId,
            meta: ctx.meta,
            method: upload.spec.method
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
    async csv(_, { uploadId, meta }) {
      /*
        Run subprocesses to stream records from Minio objects to a STAN subject.
       */
      const startedAt = new Date()
      const upload = await this.broker.call(
        'uploads.patch',
        {
          id: uploadId,
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
      const objectList = upload.result_pre.object_list

      for (let i = 0; i < objectList.length; i++) {
        const subprocess = this.spawn(
          process.execPath,
          [
            path.resolve(__dirname, '../../scripts', this.name, 'csv.js'),
            uploadId,
            i
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
          return this.patchPostError({ uploadId, err, meta, startedAt })
        }
      }

      /*
        Post-processing:
        - Patch post result.
        - Send notifications.
       */
      try {
        const finishedAt = new Date()

        const newUpload = await this.broker.call(
          'uploads.patch',
          {
            id: uploadId,
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

        if (
          newUpload.spec &&
          newUpload.spec.notify &&
          newUpload.spec.notify.length
        ) {
          await this.broker.call(
            'notification.send',
            {
              data: uploadFinishedNotification({ upload: newUpload }),
              urls: newUpload.spec.notify
            },
            { meta }
          )
        }
      } catch (err) {
        return this.patchPostError({ uploadId, err, meta, startedAt })
      }
    },

    patchPostError({ uploadId, err, meta, startedAt }) {
      const finishedAt = new Date()
      return this.broker.call(
        'uploads.patch',
        {
          id: uploadId,
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
    'file-import': {
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
