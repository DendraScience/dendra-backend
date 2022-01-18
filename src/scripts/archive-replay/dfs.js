/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module archive-replay/dfs
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const {
  createArchiveAPI,
  createResultPatcher,
  createSTANClient,
  createWebAPI,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const uploadId = process.argv[2]
const { Readable, Writable } = require('stream')

logger.info('Script is starting.')

const archiveAPI = createArchiveAPI({
  baseURL: process.env.ARCHIVE_JSON_API_URL
})
const stanClient = createSTANClient({ prefix: 'LIVE_IMPORT_STAN' })
const webAPI = createWebAPI({
  accessToken: process.env.WEB_API_ACCESS_TOKEN,
  baseURL: process.env.WEB_API_URL
})
const resultPatcher = createResultPatcher({ logger, webAPI })

let upload

function createPublisher(options, stats) {
  const context = Object.assign({}, options.context, {
    replay: true,
    replayed_at: stats.started_at,
    org_slug: upload.result_pre.org_slug,
    upload_id: uploadId
  })
  const subject = upload.result_pre.pub_to_subject

  return new Writable({
    autoDestroy: true,
    objectMode: true,
    write(document, _, done) {
      const msg = {
        context: Object.assign(
          {},
          document.content && document.content.context,
          context,
          {
            document_id: document._id
          }
        ),
        payload: document.content && document.content.payload
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

function createStats(categoryId) {
  const stats = {
    category_id: categoryId,
    publish_count: 0,
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

function handleDocumentStream(categoryId, documentStream) {
  return new Promise(resolve => {
    const { options } = upload.spec
    const stats = createStats(categoryId)
    const publisher = createPublisher(options, stats)

    const errorHandler = err => {
      statsError(stats, err)
      resultPatcher.patch().finally(resolve)
    }

    documentStream.on('error', errorHandler)
    publisher.on('error', errorHandler)

    resultPatcher.patch().finally(() => {
      documentStream.pipe(publisher).on('finish', () => {
        statsFinished(stats)
        resultPatcher.patch().finally(resolve)
      })
    })
  })
}

async function* query({ categoryId }) {
  // Fetch categories under the categoryId
  logger.debug(`Querying categories using parent_category_id ${categoryId}.`)
  const categoriesResp = await archiveAPI.get('/categories', {
    params: {
      parent_category_id: categoryId,
      $limit: 2000
    }
  })

  // Process categories recursively
  if (
    categoriesResp.data &&
    categoriesResp.data.data &&
    categoriesResp.data.data.length
  ) {
    // Process results asynchronously; 24 items at a time (hardcoded)
    const array = categoriesResp.data.data
    for (let i = 0; i < array.length; i++) {
      yield* query({ categoryId: array[i]._id })

      if (!(i % 24)) await new Promise(resolve => setImmediate(resolve))
    }
  }

  // Fetch documents for this categoryId
  logger.debug(`Querying documents using category_id ${categoryId}.`)
  const documentsResp = await archiveAPI.get('/documents', {
    params: {
      category_id: categoryId,
      $limit: 2000
    }
  })

  // Process documents
  if (
    documentsResp.data &&
    documentsResp.data.data &&
    documentsResp.data.data.length
  ) {
    // Process results asynchronously; 24 items at a time (hardcoded)
    const array = documentsResp.data.data
    for (let i = 0; i < array.length; i++) {
      // Fetch and yield document content
      const documentResp = await archiveAPI.get(`/documents/${array[i]._id}`)
      if (documentResp.data) yield documentResp.data

      if (!(i % 24)) await new Promise(resolve => setImmediate(resolve))
    }
  }
}

async function run() {
  const response = await webAPI.get(`/uploads/${uploadId}`)
  upload = response.data

  if (!upload.result_pre) throw new Error('Missing result_pre.')
  if (!upload.result) upload.result = {}
  if (!upload.result.items) upload.result.items = []

  resultPatcher.start({ result: upload.result, url: `uploads/${upload._id}` })

  const categoryId = upload.spec.options.category_id

  if (!categoryId) throw new Error('Missing category_id.')

  const documentStream = Readable.from(query({ categoryId }), {
    autoDestroy: true
  })

  await handleDocumentStream(categoryId, documentStream)
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
