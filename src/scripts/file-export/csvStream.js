/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module scripts/csvStream
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const { setupProcessHandlers } = require('../../lib/utils')
setupProcessHandlers(process, logger)

const accessToken = process.env.WEB_API_ACCESS_TOKEN
const prefixUrl = process.env.WEB_API_URL
const { names, options } = JSON.parse(process.argv[2])
const got = require('got')
const qs = require('qs')
const { query } = require('../../lib/datapoints')
const { pipeline, Readable, Transform } = require('stream')
const { createGzip } = require('zlib')
const Minio = require('minio')
const stringify = require('csv-stringify')

logger.info('Script is starting.')
logger.info(`Option begins_at: ${options.begins_at}`)
logger.info(`Option ends_before: ${options.ends_before}`)
logger.info(
  `Option datastream_ids: ${options.datastream_ids.length} datastream(s)`
)

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_END_POINT,
  port: process.env.MINIO_PORT | 0,
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY
})

const datapoints = Readable.from(
  query(
    {
      beginsAt: options.begins_at,
      endsBefore: options.ends_before,
      ids: options.datastream_ids,
      find: async params => {
        const headers = {}
        if (accessToken) headers.Authorization = accessToken

        const { body } = await got('datapoints', {
          headers,
          prefixUrl,
          responseType: 'json',
          searchParams: qs.stringify(params)
        })

        return Readable.from(body.data)
      },
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
  transform: (item, _, done) => {
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
      header: names[i] // `v${i}`
    }))
  ]
})

const gzip = createGzip()

// TODO: Column naming
// TODO: File naming
// TODO: Get/patch download (add service account auth)
// TODO: Webhook
// TODO: Update downloads resource to call webhook

minioClient
  .putObject(
    'main',
    'test.csv.zip',
    pipeline(datapoints, transform, stringifier, gzip, () => {
      logger.info('Pipeline finished.')
    })
  )
  .finally(() => {
    logger.info('Script finished.')
  })
