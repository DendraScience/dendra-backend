/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module file-import/csv
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const {
  createMinioClient,
  createSTANClient,
  isValidZipFileEntry,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const accessToken = process.env.WEB_API_ACCESS_TOKEN
const prefixUrl = process.env.WEB_API_URL
const model = JSON.parse(process.argv[2])
const { options } = model.spec
const { options: storageOptions } = model.storage
const got = require('got')
const { Transform, Writable } = require('stream')
const { createFileImportParser } = require('../../lib/csv-parse')
const unzipper = require('unzipper')
const contentType =
  model.result.object_stat &&
  model.result.object_stat.metaData &&
  model.result.object_stat.metaData['content-type']

logger.info('Script is starting.')
logger.info(`bucket_name: ${model.result.bucket_name}`)
logger.info(`object_name: ${model.result.object_name}`)
logger.info(`content-type: ${contentType}`)

const stanClient = createSTANClient('BULK_IMPORT_STAN')
const minioClient = createMinioClient()

model.result.processed_items = []

function createEntryProcessor() {
  return new Transform({
    objectMode: true,
    transform: function (entry, _, done) {
      const fileName = entry.path

      if (
        !(
          isValidZipFileEntry(entry) &&
          (fileName.endsWith('.csv') || fileName.endsWith('.dat'))
        )
      ) {
        entry.autodrain()
        done()
      } else if (
        !(
          storageOptions.file_name === undefined ||
          fileName.startsWith(storageOptions.file_name)
        )
      ) {
        logger.info(`Skipping file: ${fileName}`)

        entry.autodrain()
        done()
      } else {
        logger.info(`Parsing file: ${fileName}`)

        const stats = createStats(fileName)
        const parser = createFileImportParser(options, stats)
        const publisher = createPublisher(options, stats)

        const errorHandler = err => {
          stats.error = err.message
          patchResult(stats).finally(() => {
            entry.autodrain()
            done()
          })
        }

        parser.on('error', errorHandler)
        publisher.on('error', errorHandler)
        entry
          .pipe(parser)
          .pipe(publisher)
          .on('finish', () => {
            patchResult(stats).finally(done)
          })
      }
    }
  })
}

function createPublisher(options, stats) {
  const context = Object.assign({}, options.context, {
    file_name: stats.file_name,
    imported_at: stats.imported_at
  })

  return new Writable({
    autoDestroy: true,
    objectMode: true,
    write(payload, _, done) {
      const msgStr = JSON.stringify({
        context,
        payload
      })

      stanClient.publish(model.result.pub_to_subject, msgStr, (err, guid) => {
        if (err) stats.publish_error_count++
        else stats.publish_count++
        done()
      })
    }
  })
}

function createStats(fileName) {
  return {
    file_name: fileName,
    imported_at: new Date(),
    publish_count: 0,
    publish_error_count: 0
  }
}

function patchResult(stats) {
  logger.info(`Patching upload ${model._id} with result.`)

  const headers = {}
  if (accessToken) headers.Authorization = accessToken

  model.result.processed_items.push(stats)

  return got(`uploads/${model._id}`, {
    headers,
    json: { $set: { result: model.result } },
    method: 'PATCH',
    prefixUrl,
    responseType: 'json'
  }).catch(err => {
    logger.error(`Patch error: ${err.message}`)
  })
}

function handleFileStream(objectStream) {
  return new Promise(resolve => {
    const stats = createStats(model.result.object_name)
    const parser = createFileImportParser(options, stats)
    const publisher = createPublisher(options, stats)

    const errorHandler = err => {
      stats.error = err.message
      patchResult(stats).finally(resolve)
    }

    parser.on('error', errorHandler)
    publisher.on('error', errorHandler)
    objectStream
      .pipe(parser)
      .pipe(publisher)
      .on('finish', () => {
        patchResult(stats).finally(resolve)
      })
  })
}

function handleZipStream(objectStream) {
  return new Promise((resolve, reject) => {
    const parser = unzipper.Parse()
    const processor = createEntryProcessor()

    parser.on('error', reject)
    processor.on('error', reject)
    objectStream.pipe(parser).pipe(processor).on('finish', resolve)
  })
}

stanClient.on('error', err => {
  logger.error(`STAN error: ${err.message}`)
  process.exit(1)
})
stanClient.on('connect', () => {
  stanClient.on('connection_lost', () => {
    logger.error(`STAN connection lost.`)
    process.exit(1)
  })

  const objectStreamHandler =
    contentType === 'application/zip' ? handleZipStream : handleFileStream

  minioClient
    .getObject(model.result.bucket_name, model.result.object_name)
    .then(objectStreamHandler)
    .catch(err => {
      logger.error(`Object stream error: ${err.message}`)
    })
    .finally(() => {
      stanClient.removeAllListeners()
      stanClient.close()

      logger.info('Script finished.')
    })
})
