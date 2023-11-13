/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const ChildProcessMixin = require('../../mixins/child-process')
const QueueServiceMixin = require('moleculer-bull')
const FeathersAuthMixin = require('../../mixins/feathers-auth')
const uploadFinishedNotification = require('../../notifications/upload-finished')

module.exports = {
  name: 'file-import',

  mixins: [
    ChildProcessMixin,
    QueueServiceMixin(process.env.QUEUE_SERVICE_REDIS_URL),
    FeathersAuthMixin
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
    },
    objectExpiry: process.env.FILE_IMPORT_OBJECT_EXPIRY | 0 || 3600
  },

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
            is_active: 'boolean',
            is_cancel_requested: 'boolean',
            organization_id: 'string',
            spec: {
              type: 'object',
              props: {
                method: { type: 'enum', values: ['csv'] },
                options: {
                  type: 'object',
                  props: {
                    dry_run: { type: 'boolean', optional: true },
                    naive: { type: 'boolean', optional: true }
                  }
                }
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
                  },
                  optional: true
                }
              }
            }
          }
        }
      },
      async handler(ctx) {
        /*
          Pre-processing:
          - If missing storage options:
            - Generate presigned put URL.
            - Patch pre result.
          - Otherwise, if activated:
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

        // Switch to the service account
        const { accessToken } = await this.getAuthUser()

        if (!upload.storage.options) {
          // Generate a presigned URL for uploading the object to Minio
          const objectName = `${jobId}.dat`
          const { objectExpiry } = this.settings
          const presignedUrl = await this.broker.call(
            'minio.presignedPutObject',
            {
              bucketName,
              objectName,
              expires: objectExpiry
            }
          )
          const pre = {
            presigned_put_info: {
              expiry: objectExpiry,
              url: presignedUrl
            }
          }
          const storage = {
            method: 'minio',
            options: {
              object_limit: 1,
              object_prefix: objectName
            }
          }

          await ctx.call(
            'uploads.patch',
            {
              id: uploadId,
              data: { $set: { result_pre: pre, storage } }
            },
            {
              meta: { accessToken }
            }
          )

          return
        }

        if (upload.is_active) {
          if (
            !(await this.preFlightCheck({
              organization,
              upload,
              meta: { accessToken }
            }))
          )
            return

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

          await ctx.call(
            'uploads.patch',
            {
              id: uploadId,
              data: { $set: { result_pre: pre, state: 'queued' } }
            },
            {
              meta: { accessToken }
            }
          )

          await this.createJob(
            `${this.name}`,
            {
              uploadId,
              dryRun: upload.spec.options.dry_run,
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

    'uploads.patched': {
      strategy: 'RoundRobin',
      params: {
        before: {
          type: 'object',
          props: {
            is_active: { type: 'equal', value: false, strict: true }
          }
        },
        result: {
          type: 'object',
          props: {
            _id: 'string',
            is_active: { type: 'equal', value: true, strict: true },
            is_cancel_requested: 'boolean',
            organization_id: 'string',
            spec: {
              type: 'object',
              props: {
                method: { type: 'enum', values: ['csv'] },
                options: {
                  type: 'object',
                  props: {
                    dry_run: { type: 'boolean', optional: true },
                    naive: { type: 'boolean', optional: true }
                  }
                }
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

        // Switch to the service account
        const { accessToken } = await this.getAuthUser()

        if (
          !(await this.preFlightCheck({
            organization,
            upload,
            meta: { accessToken }
          }))
        )
          return

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

        await ctx.call(
          'uploads.patch',
          {
            id: uploadId,
            data: { $set: { result_pre: pre, state: 'queued' } }
          },
          {
            meta: { accessToken }
          }
        )

        await this.createJob(
          `${this.name}`,
          {
            uploadId,
            dryRun: upload.spec.options.dry_run,
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
    async csv(_, { uploadId, dryRun }) {
      // Switch to the service account
      const { accessToken } = await this.getAuthUser()

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
        { meta: { accessToken } }
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
                WEB_API_ACCESS_TOKEN: accessToken
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
          return this.patchPostError({
            uploadId,
            dryRun,
            err,
            meta: { accessToken },
            startedAt
          })
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
                is_active: !dryRun,
                result_post: {
                  duration: finishedAt - startedAt,
                  finished_at: finishedAt
                },
                state: 'completed'
              }
            }
          },
          { meta: { accessToken } }
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
            { meta: { accessToken } }
          )
        }
      } catch (err) {
        return this.patchPostError({
          uploadId,
          dryRun,
          err,
          meta: { accessToken },
          startedAt
        })
      }
    },

    patchPostError({ uploadId, dryRun, err, meta, startedAt }) {
      const finishedAt = new Date()
      return this.broker.call(
        'uploads.patch',
        {
          id: uploadId,
          data: {
            $set: {
              is_active: !dryRun,
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
    },

    async preFlightCheck({ organization, upload, meta }) {
      if (upload.spec.options.naive) return true

      if (
        upload.organization_id &&
        upload.station_id &&
        upload.spec.options &&
        upload.spec.options.context &&
        typeof upload.spec.options.context.source === 'string' &&
        upload.spec.options.context.source.split('/')[1] === organization.slug
      ) {
        const ids = await this.broker.call(
          'datastreams.findIds',
          {
            query: {
              source_type: 'sensor',
              station_id: upload.station_id,
              organization_id: upload.organization_id,
              'datapoints_config.params.query.source':
                upload.spec.options.context.source
            }
          },
          { meta }
        )

        if (ids.length) return true
      }

      // Fallthrough is check failed
      await this.broker.call(
        'uploads.patch',
        {
          id: upload._id,
          data: {
            $set: {
              result_pre: {
                error:
                  "Non-naive upload must specify a valid 'station_id' and 'context.source' matching at least one datastream."
              },
              state: 'error'
            }
          }
        },
        { meta }
      )
      return false
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
