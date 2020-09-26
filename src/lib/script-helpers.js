/**
 * Subprocess script helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/script-helpers
 */

const Minio = require('minio')
const STAN = require('node-nats-streaming')

function createMinioClient() {
  return new Minio.Client({
    endPoint:
      process.env.MINIO_INTERNAL_END_POINT || process.env.MINIO_END_POINT,
    port: (process.env.MINIO_INTERNAL_PORT || process.env.MINIO_PORT) | 0,
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY,
    useSSL:
      (process.env.MINIO_INTERNAL_USE_SSL || process.env.MINIO_USE_SSL) ===
      'true'
  })
}

function createSTANClient(prefix = 'STAN') {
  const url = process.env[`${prefix}_URL`] || process.env[`${prefix}_URI`]
  return STAN.connect(
    process.env[`${prefix}_CLUSTER`],
    process.env[`${prefix}_CLIENT`],
    { url }
  )
}

function isValidZipFileEntry({ path, type }) {
  const pathLower = path.toLowerCase()
  return (
    type === 'File' &&
    !(pathLower.startsWith('.') || pathLower.startsWith('__macosx'))
  )
}

function setupProcessHandlers(p, logger) {
  p.on('uncaughtException', err => {
    logger.error(`An unexpected error occurred: ${err.message}`)
    p.exit(1)
  })

  p.on('unhandledRejection', err => {
    if (!err) {
      logger.error('An unexpected empty rejection occurred')
    } else if (err instanceof Error) {
      logger.error(`An unexpected rejection occurred: ${err.message}`)
    } else {
      logger.error(`An unexpected rejection occurred: ${err.message}`)
    }
    p.exit(1)
  })
}

module.exports = {
  createMinioClient,
  createSTANClient,
  isValidZipFileEntry,
  setupProcessHandlers
}
