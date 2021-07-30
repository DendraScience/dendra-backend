/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module station-status/dp
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const {
  createMinioClient,
  createResultPatcher,
  createWebAPI,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const monitorId = process.argv[2]
const getStream = require('get-stream')
const { unescapeQuery } = require('../../lib/query')

const DEFAULT_THRESHOLD = 120
//
// TODO: Change the default before final build!!!
//
const DEFAULT_LOG_RETENTION = 2 // 1440

logger.info('Script is starting.')

const minioClient = createMinioClient()
const webAPI = createWebAPI({
  accessToken: process.env.WEB_API_ACCESS_TOKEN,
  baseURL: process.env.WEB_API_URL
})
const resultPatcher = createResultPatcher({ logger, webAPI })

let monitor

async function findStations() {
  const { options } = monitor.spec
  const orgQuery = monitor.organization_id
    ? { organization_id: monitor.organization_id }
    : {}
  const stationIds = options.station_ids
  const stationQuery = options.station_query

  let query
  if (stationQuery)
    query = unescapeQuery(Object.assign({}, stationQuery, orgQuery))
  if (stationIds && stationIds.length)
    query = query
      ? { $or: [{ _id: { $in: options.station_ids } }, query] }
      : { _id: { $in: options.station_ids } }

  // Default
  if (!query)
    query = Object.assign(
      {
        is_active: true,
        is_enabled: true,
        is_hidden: false,
        station_type: 'weather'
      },
      orgQuery
    )

  const response = await webAPI.get('/stations', {
    params: Object.assign({}, query, {
      $limit: 2000,
      $select: ['_id', 'name', 'slug'],
      $sort: { _id: 1 }
    })
  })

  return (response.data && response.data.data) || []
}

async function findStatusDatastream(stationId) {
  const { options } = monitor.spec
  const orgQuery = monitor.organization_id
    ? { organization_id: monitor.organization_id }
    : {}
  const datastreamQuery = options.datastream_query

  let query
  if (datastreamQuery)
    query = unescapeQuery(Object.assign({}, datastreamQuery, orgQuery))

  // Default
  if (!query)
    query = Object.assign(
      {
        is_enabled: true,
        $and: [
          {
            'terms_info.class_tags': 'ds_Function_Status'
          },
          {
            'terms_info.class_tags': 'ds_Medium_Battery'
          },
          {
            'terms_info.class_tags': 'ds_Variable_Voltage'
          }
        ]
      },
      orgQuery
    )

  const response = await webAPI.get('/datastreams', {
    params: Object.assign({}, query, {
      station_id: stationId,
      $limit: 1,
      $select: ['_id', 'general_config', 'general_config_resolved', 'name'],
      $sort: { _id: 1 }
    })
  })

  return (
    response.data &&
    response.data.data &&
    response.data.data.length &&
    response.data.data[0]
  )
}

async function findRecentDatapoint(datastreamId) {
  const response = await webAPI.get('/datapoints', {
    params: {
      datastream_id: datastreamId,
      $limit: 1
    }
  })

  return (
    response.data &&
    response.data.data &&
    response.data.data.length &&
    response.data.data[0]
  )
}

async function run() {
  const response = await webAPI.get(`/monitors/${monitorId}`)
  monitor = response.data

  if (!monitor.result_pre) throw new Error('Missing result_pre.')
  if (!monitor.result) monitor.result = {}

  resultPatcher.start({
    result: monitor.result,
    url: `monitors/${monitor._id}`
  })

  const bucketName = monitor.result_pre.bucket_name
  const objectName = monitor.result_pre.object_name

  if (!bucketName) throw new Error('Missing bucket_name.')
  if (!objectName) throw new Error('Missing object_name.')

  // Get report from last run
  let lastReport
  try {
    lastReport = JSON.parse(
      await getStream(await minioClient.getObject(bucketName, objectName))
    )
  } catch (_) {}

  // Construct report for this run
  const thisReport = {
    created_at: new Date(),
    items: []
  }
  const logRetention =
    (((monitor &&
      monitor.spec &&
      monitor.spec.options &&
      monitor.spec.options.log_retention) ||
      DEFAULT_LOG_RETENTION) |
      0) *
    60000
  const stations = await findStations()

  for (const station of stations) {
    let datastream
    let datastreamError
    try {
      datastream = await findStatusDatastream(station._id)
    } catch (err) {
      datastreamError = err.message
    }

    let datapoint
    let datapointError
    try {
      if (datastream) datapoint = await findRecentDatapoint(datastream._id)
    } catch (err) {
      datapointError = err.message
    }

    // Get corresponding report item from last run
    const lastItem =
      lastReport &&
      lastReport.items &&
      lastReport.items.find(
        item => item.station && item.station._id === station._id
      )

    // Get and groom log from last run
    const checkedAt = new Date()
    const lastLog =
      (lastItem &&
        lastItem.log &&
        lastItem.log.filter(
          entry => new Date(entry.checked_at) >= checkedAt - logRetention
        )) ||
      []

    // Construct new log entry for this run
    const threshold =
      (((datastream &&
        datastream.general_config_resolved &&
        datastream.general_config_resolved.station_offline_threshold) ||
        DEFAULT_THRESHOLD) |
        0) *
      60000
    const thisEntry = Object.assign(
      {
        checked_at: checkedAt,
        status: !(datapoint && datapoint.t)
          ? datastreamError || datapointError
            ? 'error'
            : 'unknown'
          : checkedAt - new Date(datapoint.t) <= threshold
          ? 'online'
          : 'offline'
      },
      datapoint ? { datapoint } : undefined,
      datapointError ? { datapoint_error: datapointError } : undefined,
      datastream ? { datastream } : undefined,
      datastreamError ? { datastream_error: datastreamError } : undefined
    )

    // Append report item for this run
    thisReport.items.push({
      log: [thisEntry, ...lastLog],
      station
    })
  }

  // TODO: Add result fields with ids of online, offline, unknown, error stations?

  await resultPatcher.patch()

  await minioClient.putObject(
    bucketName,
    objectName,
    JSON.stringify(thisReport)
  )

  await resultPatcher.patch()
}

run()
  .catch(err => {
    logger.error(`Run error: ${err.message}`)
    process.exit(1)
  })
  .finally(() => {
    resultPatcher.stop()

    logger.info('Script finished.')
  })
