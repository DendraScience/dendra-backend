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

const datastreamNameFormatters = {
  kebab: {
    shortName(data) {
      return kebabCase(data.name)
    },
    mediumName(data) {
      return kebabCase(`${data.station_lookup.slug} ${data.name}`)
    },
    longName(data) {
      return kebabCase(
        `${data.organization_lookup.slug} ${data.station_lookup.slug} ${data.name}`
      )
    },
    ordinal(name, count) {
      return count > 1 ? `${name}-${count}` : name
    }
  },
  snake: {
    shortName(data) {
      return snakeCase(data.name)
    },
    mediumName(data) {
      return snakeCase(`${data.station_lookup.slug} ${data.name}`)
    },
    longName(data) {
      return snakeCase(
        `${data.organization_lookup.slug} ${data.station_lookup.slug} ${data.name}`
      )
    },
    ordinal(name, count) {
      return count > 1 ? `${name}_${count}` : name
    }
  },
  title: {
    shortName(data) {
      return `${data.name}`
    },
    mediumName(data) {
      return `${data.station_lookup.name} ${data.name}`
    },
    longName(data) {
      return `${upperCase(data.organization_lookup.slug)} ${
        data.station_lookup.name
      } ${data.name}`
    },
    ordinal(name, count) {
      return count > 1 ? `${name} ${count}` : name
    }
  }
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
    // TODO: Add validation and defaults
    //
    async nameDatastreams(ctx) {
      // ids: [], templates: [], casing: ''
      const { format, ids } = ctx.params
      const formatter = datastreamNameFormatters[format || 'kebab']

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

      for (const batch of batches) {
        await ctx
          .call('datastreams.find', {
            _id: { $in: batch }
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

                const shortName = formatter.shortName(data)
                const mediumName = formatter.mediumName(data)
                const longName = formatter.longName(data)

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

    nameFile(ctx) {
      // ids: [], templates: [], casing: ''
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
