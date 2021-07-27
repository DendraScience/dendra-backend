/**
 * Query object encoding and transformation.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/query
 */

const { isObject, transform } = require('lodash')

function escapeKey(key) {
  return typeof key === 'string'
    ? key.replace(/~/g, '~s').replace(/\./g, '~p').replace(/^\$/g, '~d')
    : key
}

function escapeQuery(obj) {
  return transform(obj, (result, value, key) => {
    result[escapeKey(key)] = isObject(value) ? escapeQuery(value) : value
  })
}

function unescapeKey(key) {
  return typeof key === 'string'
    ? key.replace(/^~d/g, '$').replace(/~p/g, '.').replace(/~s/g, '~')
    : key
}

function unescapeQuery(obj) {
  return transform(obj, (result, value, key) => {
    result[unescapeKey(key)] = isObject(value) ? unescapeQuery(value) : value
  })
}

module.exports = {
  escapeKey,
  escapeQuery,
  unescapeKey,
  unescapeQuery
}
