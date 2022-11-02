/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const ChildProcessMixin = require('../../mixins/child-process')
const FeathersAuthMixin = require('../../mixins/feathers-auth')
const QueueServiceMixin = require('moleculer-bull')
const { defaultsDeep } = require('lodash')
const monitorCreatedNotification = require('../../notifications/monitor-created')

module.exports = {
  name: 'station-status',

  mixins: [
    ChildProcessMixin,
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
    },

    objectExpiry: process.env.STATION_STATUS_OBJECT_EXPIRY | 0 || 86400
  },

  /**
   * Actions
   */
  actions: {
    repeatables: {
      rest: {
        method: 'GET'
      },
      async handler(ctx) {
        const queue = this.getQueue(this.name)
        return queue.getRepeatableJobs()
      }
    }
  },

  /**
   * Events
   */
  events: {
    'monitors.created': {
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
                    every: 600000
                  }
                }
              }
            },
            spec_type: { type: 'equal', value: 'station/status', strict: true }
          }
        }
      },
      async handler(ctx) {
        /*
          Pre-processing:
          - Patch pre result.
          - Queue job.
         */
        const monitor = ctx.params.result
        const monitorId = monitor._id
        const jobId = `monitor-${monitorId}`
        const bucketName = 'reports'
        const objectName = `${jobId}.json`
        const organization = monitor.organization_id
          ? await ctx.call('organizations.get', {
              id: monitor.organization_id
            })
          : undefined
        const pre = Object.assign(
          {
            bucket_name: bucketName,
            object_name: objectName,
            job_id: jobId
          },
          organization ? { org_slug: organization.slug } : undefined,
          {
            queued_at: new Date()
          }
        )

        await ctx.call('monitors.patch', {
          id: monitorId,
          data: { $set: { result_pre: pre, state: 'queued' } }
        })

        await this.createJob(
          `${this.name}`,
          {
            monitorId,
            meta: ctx.meta,
            method: monitor.spec.method
          },
          {
            jobId,
            removeOnComplete: true,
            removeOnFail: true,
            repeat: monitor.spec.schedule
          }
        )

        if (monitor.spec && monitor.spec.notify && monitor.spec.notify.length) {
          await ctx.call('notification.send', {
            data: monitorCreatedNotification({ monitor }),
            urls: monitor.spec.notify
          })
        }
      }
    },

    'monitors.removed': {
      strategy: 'RoundRobin',
      params: {
        result: {
          type: 'object',
          props: {
            _id: 'string',
            spec_type: { type: 'equal', value: 'station/status', strict: true }
          }
        }
      },
      async handler(ctx) {
        const monitor = ctx.params.result
        return this.removeRepeatables(monitor._id)
      }
    }
  },

  /**
   * Methods
   */
  methods: {
    async dp(_, { monitorId, meta }) {
      /*
        Run a subprocess to output a status report to a Minio object.
       */
      const startedAt = new Date()
      let monitor
      try {
        monitor = await this.broker.call(
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
      } catch (err) {
        // Clean up if monitor is missing
        if (err.response && err.response.status === 404)
          await this.removeRepeatables(monitorId)
        throw err
      }

      const subprocess = this.spawn(
        process.execPath,
        [
          path.resolve(__dirname, '../../scripts', this.name, 'dp.js'),
          monitorId
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
        return this.patchPostError({ monitorId, err, meta, startedAt })
      }

      /*
        Post-processing:
        - Generate a presigned URL for downloading the object from Minio.
        - Patch post result.
        - Send notifications.
       */
      try {
        const { bucket_name: bucketName, object_name: objectName } =
          monitor.result_pre
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
              'response-content-type': 'application/json'
            },
            requestDate: requestDate.toISOString()
          }
        )
        const finishedAt = new Date()

        const newMonitor = await this.broker.call(
          'monitors.patch',
          {
            id: monitorId,
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
          newMonitor.spec &&
          newMonitor.spec.notify &&
          newMonitor.spec.notify.length &&
          newMonitor.result &&
          newMonitor.result.notification
        ) {
          await this.broker.call(
            'notification.send',
            {
              data: newMonitor.result.notification,
              urls: newMonitor.spec.notify
            },
            { meta }
          )
        }
      } catch (err) {
        return this.patchPostError({ monitorId, err, meta, startedAt })
      }
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
    },

    async removeRepeatables(monitorId) {
      const jobId = `monitor-${monitorId}`
      const queue = this.getQueue(this.name)
      const jobs = await queue.getRepeatableJobs()

      for (const job of jobs) {
        if (job.id === jobId) {
          this.logger.info(`Removing repeatable job with key: ${job.key}`)
          await queue.removeRepeatableByKey(job.key)
        }
      }
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
  },

  /**
   * Service started lifecycle event handler
   */
  async started() {
    this.getQueue(this.name).on('failed', (job, err) => {
      this.logger.error(
        `Queue '${this.name}' job '${job.id}' failed: ${err.message}`
      )
    })

    // Ensure monitors are scheduled
    const ids = await this.broker.call('monitors.findIds', {
      query: {
        spec_type: 'station/status'
      }
    })
    for (const id of ids) {
      const monitor = await this.broker.call('monitors.get', { id })
      const monitorId = monitor._id
      const jobId = `monitor-${monitorId}`

      await this.createJob(
        `${this.name}`,
        {
          monitorId,
          method: monitor.spec.method
        },
        {
          jobId,
          removeOnComplete: true,
          removeOnFail: true,
          repeat: monitor.spec.schedule
        }
      )
    }
  }
}
