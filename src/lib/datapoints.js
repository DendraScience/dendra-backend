/**
 * Datapoints querying and manipulation.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/datapoints
 */

class DFormatter {
  newItem() {
    return { d: {} }
  }

  setProps(item, { id }, data) {
    item.d[id] = data
  }
}

class DAFormatter {
  constructor({ length }) {
    this.empty = new Array(length).fill({})
  }

  newItem() {
    return { da: this.empty.slice() }
  }

  setProps(item, { index }, data) {
    item.da[index] = data
  }
}

class VAFormatter {
  constructor({ length }) {
    this.empty = new Array(length).fill(null)
  }

  newItem() {
    return { va: this.empty.slice() }
  }

  setProps(item, { index }, { v }) {
    item.va[index] = v
  }
}

const queryFormatters = {
  d: DFormatter,
  da: DAFormatter,
  va: VAFormatter
}

async function* query({
  beginsAt,
  concurrency = 1,
  endsBefore,
  find,
  format = 'va',
  ids,
  limit = 2016,
  logger
}) {
  const map = new Map()
  const sources = ids.map((id, index) => ({ id, index }))
  const Formatter = queryFormatters[format]

  if (!Formatter) throw new Error(`Unknown query format '${format}'.`)

  const formatter = new Formatter({ length: sources.length })

  const compareNumbers = (a, b) => a - b

  const handleArray = async (source, array) => {
    logger.trace('Array received.')

    // Process results asynchronously; 24 items at a time (hardcoded)
    for (let i = 0; i < array.length; i++) {
      processData(source, array[i])

      if (!(i % 24)) await new Promise(resolve => setImmediate(resolve))
    }
  }

  const handleStream = (source, stream) => {
    logger.trace('Stream received.')

    return new Promise((resolve, reject) => {
      stream.on('end', () => {
        logger.trace('Stream ended.')
        stream.destroy()
        resolve()
      })
      stream.on('close', () => logger.trace('Stream closed.'))
      stream.on('error', reject)
      stream.on('data', data => processData(source, data))
    }).finally(() => stream.removeAllListeners())
  }

  const processData = (source, data) => {
    const key = data.lt
    let item = map.get(key)

    if (!item) {
      item = formatter.newItem()
      if (data.o !== undefined) item.o = data.o
      if (data.t !== undefined) item.t = data.t
      if (data.lt !== undefined) item.lt = data.lt
      map.set(key, item)
    }

    delete data.o
    delete data.t
    delete data.lt

    formatter.setProps(item, source, data)

    source.lastCount++
    source.lastKey = key
  }

  const querySource = source => {
    logger.debug(`Querying source ${source.id} using key ${source.lastKey}.`)

    return find({
      datastream_id: source.id,
      time: {
        [source.lastKey ? '$gt' : '$gte']: source.lastKey
          ? source.lastKey
          : beginsAt,
        $lt: endsBefore
      },
      time_local: true,
      t_int: true,
      $limit: limit,
      $sort: {
        time: 1
      }
    })
      .then(result => {
        source.lastCount = 0
        source.lastKey = null

        return Array.isArray(result)
          ? handleArray(source, result)
          : handleStream(source, result)
      })
      .catch(err => {
        logger.error(
          `Source ${source.id} and key ${source.lastKey} returned error: ${err.message}`
        )

        source.lastCount = 0
        source.lastKey = null
      })
  }

  let minKey

  while (sources.length) {
    logger.trace(`Filtering sources with minKey ${minKey}.`)

    const filteredSources = sources.filter(
      source =>
        source.lastCount !== 0 &&
        (minKey === undefined || source.lastKey <= minKey)
    )

    logger.debug(`Querying ${filteredSources.length} sources(s).`)

    if (!filteredSources.length) break

    if (concurrency > 0) {
      while (filteredSources.length) {
        await Promise.all(
          filteredSources.splice(0, concurrency).map(querySource)
        )
      }
    } else {
      await Promise.all(filteredSources.map(querySource))
    }

    const keys = [...map.keys()].sort(compareNumbers)

    if (keys.length)
      logger.trace(
        `First key ${keys[0]} and last key ${keys[keys.length - 1]}.`
      )

    minKey = undefined

    sources.forEach(source => {
      if (source.lastKey)
        minKey =
          minKey === undefined
            ? source.lastKey
            : Math.min(source.lastKey, minKey)
    })

    logger.trace(`Assigned minKey ${minKey}.`)
    logger.debug(`Generating records for ${keys.length} key(s).`)

    let yieldCount = 0

    for (let k = 0; k < keys.length; k++) {
      const key = keys[k]

      if (key > minKey) {
        logger.trace(`Reached minKey ${minKey}.`)
        break
      }

      yieldCount++
      yield map.get(key)
      map.delete(key)
    }

    logger.debug(`Generated ${yieldCount} record(s).`)

    if (global.gc) {
      logger.debug('Requesting gc.')

      global.gc(true)
    }
  }
}

module.exports = {
  query
}
