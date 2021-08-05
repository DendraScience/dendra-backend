/**
 * HTTP agent helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/http-agent
 */

const Agent = require('agentkeepalive')
const { HttpsAgent } = require('agentkeepalive')

function agentOptions() {
  return {
    timeout: 60000,
    freeSocketTimeout: 30000
  }
}

const httpAgent = new Agent(agentOptions())
const httpsAgent = new HttpsAgent(agentOptions())

module.exports = {
  httpAgent,
  httpsAgent
}
