/**
 * Subprocess script helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/script-helpers
 */

const Agent = require('agentkeepalive')
const { HttpsAgent } = require('agentkeepalive')
const axios = require('axios')
const qs = require('qs')
const Minio = require('minio')
const STAN = require('node-nats-streaming')
const { ResultPatcher } = require('./result-patcher')

function agentOptions() {
  return {
    timeout: 60000,
    freeSocketTimeout: 30000
  }
}

const httpAgent = new Agent(agentOptions())
const httpsAgent = new HttpsAgent(agentOptions())

function createMinioClient() {
  const useSSL =
    (process.env.MINIO_INTERNAL_USE_SSL || process.env.MINIO_USE_SSL) === 'true'
  const client = new Minio.Client({
    endPoint:
      process.env.MINIO_INTERNAL_END_POINT || process.env.MINIO_END_POINT,
    port: (process.env.MINIO_INTERNAL_PORT || process.env.MINIO_PORT) | 0,
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY,
    useSSL
  })
  client.setRequestOptions({
    agent: useSSL ? httpsAgent : httpAgent
  })

  return client
}

function createResultPatcher(options) {
  return new ResultPatcher(options)
}

function createSTANClient({ prefix = 'STAN' }) {
  const url = process.env[`${prefix}_URL`] || process.env[`${prefix}_URI`]
  return STAN.connect(
    process.env[`${prefix}_CLUSTER`],
    process.env[`${prefix}_CLIENT`],
    { url }
  )
}

function createWebAPI({ accessToken, baseURL }) {
  const headers = {}
  if (accessToken) headers.Authorization = accessToken

  return axios.create({
    baseURL,
    headers,
    httpAgent,
    httpsAgent,
    maxRedirects: 0,
    paramsSerializer: function (params) {
      return qs.stringify(params)
    },
    timeout: 90000
  })
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
  createResultPatcher,
  createSTANClient,
  createWebAPI,
  isValidZipFileEntry,
  setupProcessHandlers
}
