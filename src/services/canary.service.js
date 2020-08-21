/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const path = require('path')
const stringify = require('csv-stringify')
const transform = require('stream-transform')
const CallQueueMixin = require('../mixins/call-queue')
const ChildProcessMixin = require('../mixins/child-process')
const yinanDatastreams = require('../misc/yinan-datastreams')

const accessToken = ''

module.exports = {
  name: 'canary',

  mixins: [CallQueueMixin, ChildProcessMixin],

  /**
   * Settings
   */
  settings: {},

  /**
   * Dependencies
   */
  dependencies: [],

  /**
   * Actions
   */
  actions: {
    try1: {
      params: {
        filename: 'string'
      },
      handler(ctx) {
        ctx
          .call('storage-local.get', { filename: ctx.params.filename })
          .then(stream => {
            ctx.call('minio.putObject', stream, {
              meta: {
                bucketName: 'main',
                objectName: ctx.params.filename,
                metaData: {
                  foo: 'bar'
                }
              }
            })
          })
      }
    },

    try2: {
      handler(ctx) {
        ctx
          .call('influx.query', {
            // chunked: 5,
            db: 'station_ucbo_blueoak',
            q: 'select * from source_tenmin limit 2016'
          })
          .then(stream => {
            stream.once('error', err => {
              /* eslint-disable-next-line no-console */
              console.log('ERR', err)
            })

            stream.on('data', data => {
              /* eslint-disable-next-line no-console */
              console.log('>>>', data)
            })
            // ctx.call('minio.putObject', stream, {
            //   meta: {
            //     bucketName: 'main',
            //     objectName: ctx.params.filename,
            //     metaData: {
            //       foo: 'bar'
            //     }
            //   }
            // })
          })
      }
    },

    try3: {
      handler(ctx) {
        ctx
          .call(
            'datapoints.query',
            {
              ids: [
                '5ae87927fe27f45846102b23',
                '5ae87927fe27f4e4bf102b26',
                '5ae87928fe27f4c035102b29',
                '5ae87929fe27f42e56102b2c',
                '5ae8792afe27f46490102b32',
                '5ae8792bfe27f47002102b36',
                '5ae8792cfe27f42bf9102b39',
                '5ae873d8fe27f401ae102484',
                '5ae873d9fe27f4417f10248a',
                '5ae873d9fe27f460e6102488'
              ],
              begins_at: '2019-06-30T00:00:00',
              ends_before: '2019-07-02T00:00:00'
            },
            {
              meta: {
                accessToken
              }
            }
          )
          .then(stream => {
            const transformer = transform(data => {
              return { t: data.t, lt: data.lt }
            })
            stream
              .pipe(transformer)
              .pipe(
                stringify({
                  header: true
                  // columns: {
                  //  year: 'birthYear',
                  //  phone: 'phone'
                  // }
                })
              )
              .pipe(process.stdout)
              .once('error', err => {
                /* eslint-disable-next-line no-console */
                console.log('error', err)
              })
          })
      }
    },

    try4: {
      handler(ctx) {
        ctx
          .call(
            'datastreams.find',
            {},
            {
              meta: {
                accessToken
              }
            }
          )
          .then(stream => {
            stream.once('error', err => {
              /* eslint-disable-next-line no-console */
              console.log('ERR', err)
            })

            stream.on('data', data => {
              /* eslint-disable-next-line no-console */
              console.log('>>>', data)
            })
          })
      }
    },

    try5: {
      handler() {
        return this.queueMethod('myMethod5', ['a5', 'b5'])
      }
    },

    try6: {
      handler() {
        return this.queueMethod('myMethod6', ['a6', 'b6'])
      }
    },

    try7: {
      async handler() {
        const subprocess = this.execFile(process.execPath, [
          path.resolve(__dirname, '../modules/test.js')
        ])
        // const { stdout, stderr } = await subprocess.promise

        await subprocess.promise
          .then(({ stdout, stderr }) => {
            this.logger.info('>>>', stdout, stderr)
          })
          .catch(err => {
            this.logger.error('>>>', err.message)
          })
      }
    },

    async try8(ctx) {
      const ids = [
        ...yinanDatastreams.map(datastream => datastream.airtemp_avg),
        ...yinanDatastreams.map(datastream => datastream.airtemp_max),
        ...yinanDatastreams.map(datastream => datastream.airtemp_min),
        ...yinanDatastreams.map(datastream => datastream.precip),
        ...yinanDatastreams.map(datastream => datastream.rh_avg),
        ...yinanDatastreams.map(datastream => datastream.solar_avg),
        ...yinanDatastreams.map(datastream => datastream.windspeed_avg),
        '5ae873e1fe27f4be5f1024b0'
      ]

      ctx.meta.accessToken = accessToken
      const result = await ctx.call('moniker.getDatastreamNames', {
        ids,
        format: 'kebab'
      })
      for (const item of result) {
        this.logger.info(item)
      }
    },

    try9(ctx) {
      const ids = [
        ...yinanDatastreams.map(datastream => datastream.airtemp_avg),
        ...yinanDatastreams.map(datastream => datastream.airtemp_max),
        ...yinanDatastreams.map(datastream => datastream.airtemp_min),
        ...yinanDatastreams.map(datastream => datastream.precip),
        ...yinanDatastreams.map(datastream => datastream.rh_avg),
        ...yinanDatastreams.map(datastream => datastream.solar_avg),
        ...yinanDatastreams.map(datastream => datastream.windspeed_avg)
      ]

      ctx.meta.accessToken = accessToken
      ctx.emit('download.created', {
        _id: '592f155746a1b867a114e0f0',
        spec_type: 'file/export',
        spec: {
          method: 'csvStream',
          options: {
            begins_at: '2020-05-01T00:00:00',
            ends_before: '2020-05-02T00:00:00',
            datastream_ids: ids
          }
        }
      })
    }
  },

  /**
   * Events
   */
  events: {},

  /**
   * Methods
   */
  methods: {
    myMethod5(id, a, b) {
      this.logger.info('myMethod5', id, a, b)
    },

    async myMethod6(id, a, b) {
      this.logger.info('myMethod6 hello!', id, a, b)

      return new Promise(resolve =>
        setTimeout(() => {
          this.logger.info('myMethod6 done!', id, a, b)
          resolve()
        }, 20000)
      )
    }
  },

  /**
   * Service created lifecycle event handler
   */
  created() {},

  /**
   * Service started lifecycle event handler
   */
  async started() {},

  /**
   * Service stopped lifecycle event handler
   */
  async stopped() {}
}
