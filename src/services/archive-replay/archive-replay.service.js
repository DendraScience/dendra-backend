/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const ChildProcessMixin = require('../../mixins/child-process')
const QueueServiceMixin = require('moleculer-bull')
const uploadFinishedNotification = require('../../notifications/upload-finished')

module.exports = {
  name: 'archive-replay',

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
                method: { type: 'enum', values: ['dfs'] },
                options: {
                  type: 'object',
                  props: {
                    category_id: 'string',
                    pub_suffix: {
                      type: 'array',
                      default: [],
                      items: 'string',
                      optional: true
                    }
                  }
                }
              }
            },
            spec_type: { type: 'equal', value: 'archive/replay', strict: true },
            storage: {
              type: 'object',
              props: {
                method: { type: 'equal', value: 'local', strict: true }
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
        const organization = await ctx.call('organizations.get', {
          id: upload.organization_id
        })
        const categoryId = upload.spec.options.category_id.split('-')

        // Ensure we don't replay the wrong org
        if (categoryId.length < 4 || categoryId[0] !== organization.slug) {
          await ctx.call('uploads.patch', {
            id: uploadId,
            data: {
              $set: {
                result_pre: {
                  error:
                    "Spec option 'category_id' must begin with the org slug and have at least 4 levels."
                },
                state: 'error'
              }
            }
          })
          return
        }

        const pre = {
          job_id: jobId,
          org_slug: organization.slug,
          pub_to_subject: await ctx.call('moniker.getWorkerSubject', {
            action: 'import',
            org_slug: organization.slug,
            suffix: upload.spec.options.pub_suffix,
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
    async dfs(_, { uploadId, meta }) {
      /*
        Run subprocesses to replay records from the JSON archive to a STAN subject.
       */
      const startedAt = new Date()
      await this.broker.call(
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

      const subprocess = this.spawn(
        process.execPath,
        [
          path.resolve(__dirname, '../../scripts', this.name, 'dfs.js'),
          uploadId
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
    'archive-replay': {
      concurrency: 1,
      async process({ id, data }) {
        switch (data.method) {
          case 'dfs':
            return this.dfs(id, data)

          default:
            throw new Error(`Unknown job method '${data.method}'.`)
        }
      }
    }
  }
}
