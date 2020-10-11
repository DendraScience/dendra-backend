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
const unzip = require('unzip-stream')
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
          statsError(stats, err)
          patchResult().finally(() => {
            entry.autodrain()
            done(err)
          })
        }

        parser.on('error', errorHandler)
        publisher.on('error', errorHandler)

        patchResult().finally(() => {
          entry
            .pipe(parser)
            .pipe(publisher)
            .on('finish', () => {
              statsFinished(stats)
              patchResult().finally(done)
            })
        })
      }
    }
  })
}

function createPublisher(options, stats) {
  const context = Object.assign({}, options.context, {
    file_name: stats.file_name,
    imported_at: stats.started_at
  })

  return new Writable({
    autoDestroy: true,
    objectMode: true,
    write(payload, _, done) {
      const msg = {
        context,
        payload
      }
      const msgStr = JSON.stringify(msg)

      stanClient.publish(model.result.pub_to_subject, msgStr, err => {
        if (err) {
          done(err)
        } else {
          stats.publish_count++
          done()
        }
      })
    }
  })
}

model.result.items = []

function createStats(fileName) {
  const stats = {
    file_name: fileName,
    publish_count: 0,
    started_at: new Date(),
    state: 'started'
  }
  model.result.items.push(stats)
  return stats
}

function statsError(stats, err) {
  const finishedAt = new Date()
  stats.duration = finishedAt - stats.started_at
  stats.error = err.message
  stats.finished_at = finishedAt
  stats.state = 'error'
}

function statsFinished(stats) {
  const finishedAt = new Date()
  stats.duration = finishedAt - stats.started_at
  stats.finished_at = finishedAt
  stats.state = 'finished'
}

const patchResultTimer = setTimeout(() => {
  patchResult()
}, 120000)

function patchResult() {
  patchResultTimer.refresh()

  const headers = {}
  if (accessToken) headers.Authorization = accessToken

  return got(`uploads/${model._id}`, {
    headers,
    json: { $set: { result: model.result } },
    method: 'PATCH',
    prefixUrl,
    responseType: 'json'
  }).catch(err => {
    logger.error(`Patch error: ${err.message}`)
    process.exit(1)
  })
}

function handleFileStream(objectStream) {
  return new Promise(resolve => {
    const stats = createStats(model.result.object_name)
    const parser = createFileImportParser(options, stats)
    const publisher = createPublisher(options, stats)

    const errorHandler = err => {
      statsError(stats, err)
      patchResult().finally(resolve)
    }

    objectStream.on('error', errorHandler)
    parser.on('error', errorHandler)
    publisher.on('error', errorHandler)

    patchResult().finally(() => {
      objectStream
        .pipe(parser)
        .pipe(publisher)
        .on('finish', () => {
          statsFinished(stats)
          patchResult().finally(resolve)
        })
    })
  })
}

function handleZipStream(objectStream) {
  return new Promise((resolve, reject) => {
    const parser = unzip.Parse()
    const processor = createEntryProcessor()

    objectStream.on('error', reject)
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
      clearTimeout(patchResultTimer)
      stanClient.removeAllListeners()
      stanClient.close()

      logger.info('Script finished.')
    })
})
