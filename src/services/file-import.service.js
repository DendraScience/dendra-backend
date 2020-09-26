/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const CallQueueMixin = require('../mixins/call-queue')
const ChildProcessMixin = require('../mixins/child-process')
const { merge } = require('lodash')

module.exports = {
  name: 'file-import',

  mixins: [CallQueueMixin, ChildProcessMixin],

  /**
   * Settings
   */
  settings: {},

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
    'uploads.created': {
      strategy: 'Shard',
      strategyOptions: {
        shardKey: '_id'
      },
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
                    object_name: 'string'
                  }
                }
              }
            }
          }
        }
      },
      async handler(ctx) {
        const { result } = ctx.params
        const bucketName = this.name
        const objectName = result.storage.options.object_name
        const model = merge(
          {
            spec: {
              options: {
                context: {}
              }
            },
            storage: {
              options: {}
            }
          },
          result,
          {
            result: {
              bucket_name: bucketName,
              object_name: objectName,
              object_stat: await ctx.call('minio.statObject', {
                bucketName,
                objectName
              })
            }
          }
        )

        // Add org_slug to context
        const organization = await ctx.call('organizations.get', {
          id: result.organization_id
        })
        model.spec.options.context.org_slug = organization.slug
        model.spec.options.context.upload_id = result._id

        // Get streaming subject for bulk import, e.g. 'erczo.import.v1.out.file'
        model.result.pub_to_subject = await ctx.call(
          'moniker.getWorkerSubject',
          {
            action: 'import',
            org_slug: organization.slug,
            suffix: ['file'],
            type: 'out'
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
        'uploads.patch',
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

        const upload = await this.broker.call(
          'uploads.get',
          { id: model._id },
          { meta }
        )
        const finishedAt = new Date()
        upload.result.duration = finishedAt - startedAt
        upload.result.error = err.message
        upload.result.finished_at = finishedAt
        upload.result.state = 'error-subprocess'
        upload.result.subprocess_id = subprocess.id
        await this.broker.call(
          'uploads.patch',
          {
            id: model._id,
            data: { $set: { result: upload.result } }
          },
          { meta }
        )

        return
      }

      const upload = await this.broker.call(
        'uploads.get',
        { id: model._id },
        { meta }
      )
      const finishedAt = new Date()
      upload.result.duration = finishedAt - startedAt
      upload.result.finished_at = finishedAt
      upload.result.state = 'finished'
      upload.result.subprocess_id = subprocess.id
      await this.broker.call(
        'uploads.patch',
        {
          id: model._id,
          data: { $set: { result: upload.result } }
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
