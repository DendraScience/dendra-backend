/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const got = require('got')
const { Readable } = require('stream')

async function* query(ctx, service) {
  const { body } = await got('query', {
    prefixUrl: ctx.meta.url || service.settings.url,
    responseType: 'json',
    searchParams: ctx.params
  })

  if (body.error) throw new Error(body.error)

  const resultsArr = body.results

  if (!resultsArr) throw new Error('results missing')

  for (let r = 0; r < resultsArr.length; r++) {
    const resultsObj = resultsArr[r]

    if (resultsObj.error) throw new Error(resultsObj.error)

    yield {
      result: r,
      statement_id: resultsObj.statement_id
    }

    const seriesArr = resultsObj.series

    if (seriesArr) {
      for (let s = 0; s < seriesArr.length; s++) {
        const seriesObj = seriesArr[s]

        if (!seriesObj.name) throw new Error('series name missing')
        if (!seriesObj.columns) throw new Error('series columns missing')

        yield {
          result: r,
          series: s,
          name: seriesObj.name,
          columns: seriesObj.columns
        }

        const valuesArr = seriesObj.values

        if (valuesArr) {
          for (let v = 0; v < valuesArr.length; v++) {
            // TODO: Determine if this is beneficial
            // if (!(v % 24)) await new Promise(resolve => setImmediate(resolve))

            yield {
              result: r,
              series: s,
              name: seriesObj.name,
              values: valuesArr[v]
            }
          }
        }
      }
    }
  }
}

module.exports = {
  name: 'influx',

  /**
   * Settings
   */
  settings: {
    url: 'http://localhost:8086'
  },

  /**
   * Dependencies
   */
  dependencies: [],

  /**
   * Actions
   */
  actions: {
    query: {
      async handler(ctx) {
        return Readable.from(query(ctx, this))
      }
    }
  },

  /**
   * Events
   */
  events: {},

  /**
   * Methods
   */
  methods: {},

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
