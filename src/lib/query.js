/**
 * Query object encoding and transformation.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/query
 */

const { isObject, transform } = require('lodash')

function escapeKey(key) {
  return key.replace(/~/g, '~s').replace(/\./g, '~p').replace(/^\$/g, '~d')
}

function unescapeKey(key) {
  return key.replace(/^~d/g, '$').replace(/~p/g, '.').replace(/~s/g, '~')
}

function transformQuery(obj) {
  return transform(obj, (result, value, key) => {
    result[escapeKey(key)] = isObject(value) ? transformQuery(value) : value
  })
}

module.exports = {
  escapeKey,
  unescapeKey,
  transformQuery
}
