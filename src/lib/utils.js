/**
 * Utilities and helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/utils
 */

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
  setupProcessHandlers
}
