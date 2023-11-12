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
  createResultPatcher,
  createSTANClient,
  createWebAPI,
  isValidZipFileEntry,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const uploadId = process.argv[2]
const objectIndex = process.argv[3] | 0
const getStream = require('get-stream')
const { Readable, Transform, Writable } = require('stream')
const { createFileImportParser } = require('../../lib/csv-parse')
const { createGunzip } = require('zlib')
const unzip = require('unzip-stream')

logger.info('Script is starting.')

const stanClient = createSTANClient({ prefix: 'BULK_IMPORT_STAN' })
const minioClient = createMinioClient()
const webAPI = createWebAPI({
  accessToken: process.env.WEB_API_ACCESS_TOKEN,
  baseURL: process.env.WEB_API_URL
})
const resultPatcher = createResultPatcher({ logger, webAPI })

let upload

function createEntryProcessor() {
  const { options } = upload.spec
  const { options: storageOptions } = upload.storage

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
          storageOptions.file_prefix === undefined ||
          fileName.startsWith(storageOptions.file_prefix)
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
          logger.error(`EntryProcessor error: ${err.message}`)

          statsError(stats, err)
          resultPatcher.patch().finally(() => {
            entry.autodrain()
            done(err)
          })
        }

        parser.on('error', errorHandler)
        publisher.on('error', errorHandler)

        resultPatcher.patch().finally(() => {
          entry
            .pipe(parser)
            .pipe(publisher)
            .on('finish', () => {
              statsFinished(stats)
              resultPatcher.patch().finally(done)
            })
        })
      }
    }
  })
}

function createPublisher(options, stats) {
  const context = Object.assign({}, options.context, {
    file_name: stats.file_name,
    imported_at: stats.started_at,
    org_slug: upload.result_pre.org_slug,
    upload_id: uploadId
  })
  const subject = upload.result_pre.pub_to_subject

  return new Writable({
    autoDestroy: true,
    objectMode: true,
    write(payload, _, done) {
      if (stats.sampled_data.length < 10) stats.sampled_data.push(payload)

      if (options.dry_run) return done()

      const msg = {
        context,
        payload
      }
      const msgStr = JSON.stringify(msg)

      stanClient.publish(subject, msgStr, err => {
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

function createStats(fileName) {
  const stats = {
    file_name: fileName,
    publish_count: 0,
    sampled_data: [],
    started_at: new Date(),
    state: 'processing'
  }
  upload.result.items.push(stats)
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
  stats.state = 'completed'
}

function handleFileStream(objectInfo, objectStream) {
  return new Promise(resolve => {
    const fileName = objectInfo.name
    const { options } = upload.spec
    const stats = createStats(fileName)
    const parser = createFileImportParser(options, stats)
    const publisher = createPublisher(options, stats)

    const errorHandler = err => {
      logger.error(`FileStream error: ${err.message}`)

      statsError(stats, err)
      resultPatcher.patch().finally(resolve)
    }

    objectStream.on('error', errorHandler)
    parser.on('error', errorHandler)
    publisher.on('error', errorHandler)

    resultPatcher.patch().finally(() => {
      objectStream
        .pipe(parser)
        .pipe(publisher)
        .on('finish', () => {
          statsFinished(stats)
          resultPatcher.patch().finally(resolve)
        })
    })
  })
}

function handleGzipStream(objectInfo, objectStream) {
  return new Promise(resolve => {
    const fileName = objectInfo.name
    const { options } = upload.spec
    const stats = createStats(fileName)
    const gunzip = createGunzip()
    const parser = createFileImportParser(options, stats)
    const publisher = createPublisher(options, stats)

    const errorHandler = err => {
      logger.error(`GzipStream error: ${err.message}`)

      statsError(stats, err)
      resultPatcher.patch().finally(resolve)
    }

    objectStream.on('error', errorHandler)
    gunzip.on('error', errorHandler)
    parser.on('error', errorHandler)
    publisher.on('error', errorHandler)

    resultPatcher.patch().finally(() => {
      objectStream
        .pipe(gunzip)
        .pipe(parser)
        .pipe(publisher)
        .on('finish', () => {
          statsFinished(stats)
          resultPatcher.patch().finally(resolve)
        })
    })
  })
}

function handleZipStream(objectInfo, objectStream) {
  return new Promise((resolve, reject) => {
    const parser = unzip.Parse()
    const processor = createEntryProcessor()

    objectStream.on('error', reject)
    parser.on('error', reject)
    processor.on('error', reject)
    objectStream.pipe(parser).pipe(processor).on('finish', resolve)
  })
}

async function* getObject({
  bucketName,
  chunkSize = 4096,
  maxRetryCount = 3,
  maxRetryDelay = 3000,
  objectInfo
}) {
  const { name, size } = objectInfo
  let offset = 0

  while (offset < size) {
    const length = Math.min(chunkSize, size - offset)
    let buffer
    let count = 0

    while (true) {
      try {
        const stream = await minioClient.getPartialObject(
          bucketName,
          name,
          offset,
          length
        )
        buffer = await getStream.buffer(stream)
        break
      } catch (err) {
        if (count++ >= maxRetryCount) throw err

        await new Promise(resolve => setTimeout(resolve, maxRetryDelay))
      }
    }

    yield buffer

    offset += chunkSize
  }
}

async function run() {
  const resp = await webAPI.get(`/uploads/${uploadId}`)
  upload = resp.data

  if (!upload.result_pre) throw new Error('Missing result_pre.')
  if (!upload.result) upload.result = {}
  if (!upload.result.items) upload.result.items = []

  resultPatcher.start({ result: upload.result, url: `uploads/${upload._id}` })

  const bucketName = upload.result_pre.bucket_name
  const objectList = upload.result_pre.object_list

  if (!bucketName) throw new Error('Missing bucket_name.')
  if (!objectList) throw new Error('Missing object_list.')

  const objectInfo = objectList[objectIndex]

  if (!objectInfo) throw new Error('Object info undefined.')
  if (!objectInfo.metadata) throw new Error('Missing object metadata.')

  const contentType = objectInfo.metadata['content-type']

  if (!contentType) throw new Error('Missing content-type metadata.')

  const objectStream = Readable.from(getObject({ bucketName, objectInfo }))

  if (contentType.includes('application/x-gzip'))
    await handleGzipStream(objectInfo, objectStream)
  else if (contentType.includes('application/zip'))
    await handleZipStream(objectInfo, objectStream)
  else await handleFileStream(objectInfo, objectStream)
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

  run()
    .catch(err => {
      logger.error(`Run error: ${err.message}`)
      process.exit(1)
    })
    .finally(() => {
      resultPatcher.stop()
      stanClient.removeAllListeners()
      stanClient.close()

      logger.info('Script finished.')
    })
})
