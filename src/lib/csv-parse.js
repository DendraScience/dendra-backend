/**
 * CSV parse factory helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/csv-parse
 */

const parse = require('csv-parse')
const moment = require('moment')
const { pick } = require('lodash')

function createFileImportParser(options, stats) {
  const {
    cast_nan: castNaN = true,
    cast_null: castNull = false,
    columns_case: columnsCase,
    columns_map: columnsMap,
    columns_name: columnsName,
    date_column: dateColumn,
    skip_columns: skipColumns = [],
    skip_lines: skipLines = {},
    time_adjust: timeAdjust,
    time_column: timeColumn,
    time_format: timeFormat
  } = options

  const mapColumn = column => {
    let col = `${column}`
    if (typeof columnsMap === 'object') col = columnsMap[col] || col
    if (columnsName === 'safe') col = col.replace(/\W/g, '_')
    if (columnsCase === 'lower') col = col.toLowerCase()
    else if (columnsCase === 'upper') col = col.toUpperCase()
    return col
  }
  const columns =
    columnsCase || columnsMap || columnsName
      ? header => header.map(mapColumn)
      : true
  const nanValues =
    castNaN === true
      ? ['NAN', 'NaN']
      : typeof castNaN === 'string'
      ? [castNaN]
      : Array.isArray(castNaN)
      ? castNaN
      : []
  const nullValues =
    castNull === true
      ? ['NULL', 'null']
      : typeof castNull === 'string'
      ? [castNull]
      : Array.isArray(castNull)
      ? castNull
      : []
  const timeColumns =
    typeof timeColumn === 'string'
      ? [timeColumn]
      : ['TIME', 'time', 'TIMESTAMP', 'timestamp']

  stats.record_count = 0
  stats.skipped_record_count = 0

  // On record handler
  const onRecord = (record, { lines }) => {
    let newRecord = Object.assign({}, record)

    // Check for skipped lines
    if (skipLines) {
      const { at, from, to } = skipLines
      if (Array.isArray(at) && at.includes(lines)) newRecord = null
      if (typeof from === 'number' && lines >= from) {
        if (typeof to !== 'number' || lines <= to) newRecord = null
      }
    }

    if (newRecord) {
      // Check for skipped columns
      if (skipColumns) skipColumns.forEach(name => delete newRecord[name])

      // Cast time column
      const timeColFound = timeColumns.find(
        name => newRecord[name] !== undefined
      )
      if (!timeColFound) throw new Error('Time column not found')

      let time = newRecord[timeColFound]

      if (typeof dateColumn === 'string') {
        const date = newRecord[dateColumn]
        if (date === undefined) throw new Error('Date column not found')
        time = `${date} ${time}`
      }

      time =
        typeof timeFormat === 'string'
          ? moment.utc(time, timeFormat)
          : moment.utc(time)
      if (typeof timeAdjust === 'number') time.add(timeAdjust, 's')

      if (!time.isValid()) throw new Error('Time value not valid')

      delete newRecord[timeColFound]
      newRecord.time = time.valueOf()

      stats.time_max =
        stats.time_max === undefined
          ? newRecord.time
          : Math.max(stats.time_max, newRecord.time)
      stats.time_min =
        stats.time_min === undefined
          ? newRecord.time
          : Math.min(stats.time_min, newRecord.time)

      // Cast NaN and null
      Object.keys(newRecord).forEach(name => {
        if (name === timeColFound) return
        if (nanValues.includes(newRecord[name])) newRecord[name] = NaN
        else if (nullValues.includes(newRecord[name])) newRecord[name] = null
      })

      stats.record_count++
    } else {
      stats.skipped_record_count++
    }

    return newRecord
  }

  /*
    Configure parser.
   */

  const parseOptions = Object.assign(
    {
      cast: true,
      columns,
      on_record: onRecord
    },
    pick(options, [
      // SEE: https://csv.js.org/parse/options/
      'bom',
      'cast',
      'columns',
      'comment',
      'delimiter',
      'escape',
      'from_line',
      'ltrim',
      'max_record_size',
      'quote',
      'relax',
      'relax_column_count',
      'relax_column_count_less',
      'relax_column_count_more',
      'record_delimiter',
      'rtrim',
      'skip_empty_lines',
      'skip_lines_with_error',
      'skip_lines_with_empty_values',
      'trim'
    ])
  )

  return parse(parseOptions)
}

module.exports = {
  createFileImportParser
}
