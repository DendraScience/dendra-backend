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
  createHTTPClient,
  createMinioClient,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const downloadId = process.argv[2]
const getStream = require('get-stream')
const { query } = require('../../lib/datapoints')
const { pipeline, Readable, Transform } = require('stream')
const { createGzip } = require('zlib')
const stringify = require('csv-stringify')

logger.info('Script is starting.')

const minioClient = createMinioClient()
const webAPI = createHTTPClient({
  accessToken: process.env.WEB_API_ACCESS_TOKEN,
  baseURL: process.env.WEB_API_URL
})

let download

const patchResultTimer = setTimeout(() => {
  patchResult()
}, 50000)

function patchResult() {
  patchResultTimer.refresh()

  return webAPI
    .patch(`downloads/${download._id}`, {
      $set: {
        result: download.result,
        state: 'running'
      }
    })
    .catch(err => {
      logger.error(`Patch error: ${err.message}`)
      process.exit(1)
    })
}

function createDatapoints(options) {
  return Readable.from(
    query(
      {
        beginsAt: options.begins_at,
        concurrency: options.concurrency,
        endsBefore: options.ends_before,
        find: async params => {
          let body
          let count = 0

          while (true) {
            try {
              const response = await webAPI.get('/datapoints', {
                params
              })
              body = response.data
              break
            } catch (err) {
              download.result.datapoints_get_error_count++

              if (count++ >= options.max_retry_count) throw err

              download.result.datapoints_get_retry_count++
              await new Promise(resolve =>
                setTimeout(resolve, options.max_retry_delay)
              )
            }
          }

          download.result.datapoints_get_success_count++
          download.result.datapoints_count += body.data.length

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
}

function createTransform() {
  return new Transform({
    autoDestroy: true,
    objectMode: true,
    transform(item, _, done) {
      download.result.recent_time = item.lt
      download.result.record_count++

      const str = new Date(item.lt).toISOString()
      item.lt = `${str.slice(0, 10)} ${str.slice(11, 19)}`
      done(null, item)
    }
  })
}

function createStringifier(options) {
  return stringify({
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
}

async function run() {
  const response = await webAPI.get(`/downloads/${downloadId}`)
  download = response.data

  if (!download.result_pre) throw new Error('Missing result_pre.')
  if (!download.result) download.result = {}
  download.result.datapoints_count = 0
  download.result.datapoints_get_error_count = 0
  download.result.datapoints_get_retry_count = 0
  download.result.datapoints_get_success_count = 0
  download.result.record_count = 0

  const bucketName = download.result_pre.bucket_name
  const objectName = download.result_pre.object_name

  if (!bucketName) throw new Error('Missing bucket_name.')
  if (!objectName) throw new Error('Missing object_name.')

  const options = JSON.parse(
    await getStream(
      await minioClient.getObject(bucketName, `${objectName}.json`)
    )
  )
  const datapoints = createDatapoints(options)
  const transform = createTransform()
  const stringifier = createStringifier(options)
  const gzip = createGzip()

  await patchResult()

  const objectStream = pipeline(
    datapoints,
    transform,
    stringifier,
    gzip,
    () => {
      logger.info('Pipeline finished.')
    }
  )

  await minioClient.putObject(bucketName, objectName, objectStream)
}

run()
  .catch(err => {
    logger.error(`Run error: ${err.message}`)
    process.exit(1)
  })
  .finally(() => {
    clearTimeout(patchResultTimer)

    logger.info('Script finished.')
  })
