/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module file-export/csv
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const {
  createMinioClient,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const accessToken = process.env.WEB_API_ACCESS_TOKEN
const prefixUrl = process.env.WEB_API_URL
const model = JSON.parse(process.argv[2])
const { options } = model.spec
// const http = require('http')
// const https = require('https')
const Agent = require('agentkeepalive')
const { HttpsAgent } = require('agentkeepalive')
const axios = require('axios')
const qs = require('qs')
const { query } = require('../../lib/datapoints')
const { pipeline, Readable, Transform } = require('stream')
const { createGzip } = require('zlib')
const stringify = require('csv-stringify')

logger.info('Script is starting.')
logger.info(`begins_at: ${options.begins_at}`)
logger.info(`ends_before: ${options.ends_before}`)
logger.info(`datastream_ids: ${options.datastream_ids.length} datastream(s)`)
logger.info(`concurrency: ${options.concurrency}`)
logger.info(`limit: ${options.limit}`)
logger.info(`bucket_name: ${model.result.bucket_name}`)
logger.info(`object_name: ${model.result.object_name}`)

model.result.datapoints_count = 0
model.result.datapoints_get_count = 0
model.result.record_count = 0

const api = createHTTPClient()

const patchResultTimer = setTimeout(() => {
  patchResult()
}, 40000)

function createHTTPClient() {
  const headers = {}
  if (accessToken) headers.Authorization = accessToken

  return axios.create({
    baseURL: prefixUrl,
    headers,
    httpAgent: new Agent({
      timeout: 60000,
      freeSocketTimeout: 30000
    }),
    httpsAgent: new HttpsAgent({
      timeout: 60000,
      freeSocketTimeout: 30000
    }),
    // httpAgent: new http.Agent({ keepAlive: true }),
    // httpsAgent: new https.Agent({ keepAlive: true }),
    maxRedirects: 0,
    paramsSerializer: function (params) {
      return qs.stringify(params)
    },
    timeout: 180000
  })
}

function patchResult() {
  patchResultTimer.refresh()

  return api
    .patch(`downloads/${model._id}`, { $set: { result: model.result } })
    .catch(err => {
      logger.error(`Patch error: ${err.message}`)
      process.exit(1)
    })
}

const datapoints = Readable.from(
  query(
    {
      beginsAt: options.begins_at,
      concurrency: options.concurrency,
      endsBefore: options.ends_before,
      find: async params => {
        const response = await api.get('/datapoints', {
          params
        })
        const body = response.data

        model.result.datapoints_count += body.data.length
        model.result.datapoints_get_count++

        return body.data
      },
      ids: options.datastream_ids,
      limit: options.limit,
      logger
    },
    {
      autoDestroy: true
    }
  )
)

const transform = new Transform({
  autoDestroy: true,
  objectMode: true,
  transform(item, _, done) {
    model.result.recent_time = item.lt
    model.result.record_count++

    const str = new Date(item.lt).toISOString()
    item.lt = `${str.slice(0, 10)} ${str.slice(11, 19)}`
    done(null, item)
  }
})

const stringifier = stringify({
  autoDestroy: true,
  header: true,
  columns: [
    { key: 'lt', header: 'time' },
    ...options.datastream_ids.map((id, i) => ({
      key: `va[${i}]`,
      header: (options.column_names && options.column_names[i]) || id
    }))
  ]
})

const gzip = createGzip()

const minioClient = createMinioClient()

minioClient
  .putObject(
    model.result.bucket_name,
    model.result.object_name,
    pipeline(datapoints, transform, stringifier, gzip, () => {
      logger.info('Pipeline finished.')
    })
  )
  .then(() => {
    return patchResult()
  })
  .finally(() => {
    clearTimeout(patchResultTimer)

    logger.info('Script finished.')
  })
