/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

const { kebabCase, snakeCase, upperCase } = require('lodash')

class Counter {
  constructor() {
    this.map = new Map()
  }

  inc(key) {
    const count = (this.map.get(key) | 0) + 1
    this.map.set(key, count)
    return count
  }
}

function abbrToAlpha(str) {
  return (
    str &&
    str
      .replace(/[^a-z0-9]/gi, x => {
        return x === '%'
          ? 'pct'
          : x === '°'
          ? 'deg'
          : x === 'µ'
          ? 'u'
          : x === 'Ω'
          ? 'ohm'
          : ''
      })
      .toLowerCase()
  )
}

function concatNames(...args) {
  return args.filter(arg => arg).join(' ')
}

const datastreamNameFormatters = {
  kebab: {
    shortName(data, unitTerm) {
      return kebabCase(
        concatNames(data.name, unitTerm && abbrToAlpha(unitTerm.abbreviation))
      )
    },
    mediumName(data, unitTerm) {
      return kebabCase(
        concatNames(
          data.station_lookup && data.station_lookup.slug,
          data.name,
          unitTerm && abbrToAlpha(unitTerm.abbreviation)
        )
      )
    },
    longName(data, unitTerm) {
      return kebabCase(
        concatNames(
          data.organization_lookup && data.organization_lookup.slug,
          data.station_lookup && data.station_lookup.slug,
          data.name,
          unitTerm && abbrToAlpha(unitTerm.abbreviation)
        )
      )
    },
    ordinal(name, count) {
      return count > 1 ? `${name}--${count}` : name
    }
  },
  snake: {
    shortName(data, unitTerm) {
      return snakeCase(
        concatNames(data.name, unitTerm && abbrToAlpha(unitTerm.abbreviation))
      )
    },
    mediumName(data, unitTerm) {
      return snakeCase(
        concatNames(
          data.station_lookup && data.station_lookup.slug,
          data.name,
          unitTerm && abbrToAlpha(unitTerm.abbreviation)
        )
      )
    },
    longName(data, unitTerm) {
      return snakeCase(
        concatNames(
          data.organization_lookup && data.organization_lookup.slug,
          data.station_lookup && data.station_lookup.slug,
          data.name,
          unitTerm && abbrToAlpha(unitTerm.abbreviation)
        )
      )
    },
    ordinal(name, count) {
      return count > 1 ? `${name}__${count}` : name
    }
  },
  title: {
    shortName(data, unitTerm) {
      return concatNames(data.name, unitTerm && unitTerm.abbreviation)
    },
    mediumName(data, unitTerm) {
      return concatNames(
        data.station_lookup && data.station_lookup.name,
        data.name,
        unitTerm && unitTerm.abbreviation
      )
    },
    longName(data, unitTerm) {
      return concatNames(
        upperCase(data.organization_lookup && data.organization_lookup.slug),
        data.station_lookup && data.station_lookup.name,
        data.name,
        unitTerm && unitTerm.abbreviation
      )
    },
    ordinal(name, count) {
      return count > 1 ? `${name} (${count})` : name
    }
  }
}

function dateFromObjectId(objectId) {
  return new Date(parseInt(objectId.substring(0, 8), 16) * 1000)
}

module.exports = {
  name: 'moniker',

  /**
   * Settings
   */
  // settings: {},

  /**
   * Dependencies
   */
  // dependencies: [],

  /**
   * Actions
   */
  actions: {
    // TODO: Add params validation and defaults
    async getDatastreamNames(ctx) {
      const { format = 'kebab', ids = [] } = ctx.params
      const formatter = datastreamNameFormatters[format]

      if (!formatter) throw new Error(`Unknown name format '${format}'.`)

      const batches = []
      const batchSize = 100
      const organizationIdCounter = new Counter()
      const stationIdCounter = new Counter()
      const shortNameCounter = new Counter()
      const mediumNameCounter = new Counter()
      const longNameCounter = new Counter()
      const shortNames = []
      const mediumNames = []
      const longNames = []

      for (let i = 0; i < ids.length; i += batchSize)
        batches.push(ids.slice(i, i + batchSize))

      const unitTermsByTag = await ctx.call('vocabularies.getUnitTermsByTag')

      for (const batch of batches) {
        await ctx
          .call('datastreams.find', {
            query: { _id: { $in: batch } }
          })
          .then(stream => {
            return new Promise((resolve, reject) => {
              stream.on('end', () => {
                this.logger.trace('Stream ended.')
                stream.destroy()
                resolve()
              })
              stream.on('close', () => this.logger.trace('Stream closed.'))
              stream.on('error', reject)
              stream.on('data', data => {
                organizationIdCounter.inc(data.organization_id)
                stationIdCounter.inc(data.station_id)

                const unitTerm =
                  data.terms_info &&
                  data.terms_info.unit_tag &&
                  unitTermsByTag[data.terms_info.unit_tag]

                const shortName = formatter.shortName(data, unitTerm)
                const mediumName = formatter.mediumName(data, unitTerm)
                const longName = formatter.longName(data, unitTerm)

                const shortNameCount = shortNameCounter.inc(shortName)
                const mediumNameCount = mediumNameCounter.inc(mediumName)
                const longNameCount = longNameCounter.inc(longName)

                shortNames.push(formatter.ordinal(shortName, shortNameCount))
                mediumNames.push(formatter.ordinal(mediumName, mediumNameCount))
                longNames.push(formatter.ordinal(longName, longNameCount))
              })
            })
          })
      }

      if (organizationIdCounter.map.size > 1) return longNames
      if (stationIdCounter.map.size > 1) return mediumNames
      return shortNames
    },

    getObjectName(ctx) {
      if (
        ctx.params._id &&
        ctx.params.spec_type === 'file/export' &&
        ctx.params.spec &&
        ctx.params.spec.method === 'csvStream'
      ) {
        let fileName
        if (ctx.params.spec.options && ctx.params.spec.options.file_name) {
          fileName = ctx.params.spec.options.file_name
        } else {
          const str = dateFromObjectId(ctx.params._id).toISOString()
          fileName = `${ctx.params.spec.method} ${str.slice(0, 10)} ${str.slice(
            11,
            19
          )}`
        }
        return `${kebabCase(fileName)}.${ctx.params._id}.csv.zip`
      }

      throw new Error('Unknown object type.')
    }
  }

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  // methods: {},

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
