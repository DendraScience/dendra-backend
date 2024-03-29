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
const stationStatusChangeNotification = require('../../notifications/station-status-change')

const DEFAULT_THRESHOLD = 120 // 2 hours
const DEFAULT_LOG_RETENTION = 1440 // 24 hours

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

  const resp = await webAPI.get('/stations', {
    params: Object.assign({}, query, {
      $limit: 2000,
      $select: ['_id', 'name', 'slug', 'time_zone'],
      $sort: { _id: 1 }
    })
  })

  return (resp.data && resp.data.data) || []
}

async function findDatastream(stationId) {
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

  const resp = await webAPI.get('/datastreams', {
    params: Object.assign({}, query, {
      station_id: stationId,
      $limit: 1,
      $select: ['_id', 'general_config', 'general_config_resolved', 'name'],
      $sort: { _id: 1 }
    })
  })

  return (
    resp.data && resp.data.data && resp.data.data.length && resp.data.data[0]
  )
}

async function findDatapoint(datastreamId) {
  const options = Object.assign(
    {
      max_retry_count: 5,
      max_retry_delay: 5000
    },
    monitor.spec.options
  )
  let body
  let count = 0

  while (true) {
    try {
      const resp = await webAPI.get('/datapoints', {
        params: {
          datastream_id: datastreamId,
          $limit: 1
        }
      })
      body = resp.data
      break
    } catch (err) {
      monitor.result.datapoints_get_error_count++

      if (count++ >= options.max_retry_count) throw err

      monitor.result.datapoints_get_retry_count++
      await new Promise(resolve => setTimeout(resolve, options.max_retry_delay))
    }
  }

  monitor.result.datapoints_get_success_count++

  return body && body.data && body.data.length && body.data[0]
}

async function run() {
  const resp = await webAPI.get(`/monitors/${monitorId}`)
  monitor = resp.data

  if (!monitor.result_pre) throw new Error('Missing result_pre.')
  if (!monitor.result) monitor.result = {}
  monitor.result.datapoints_get_error_count = 0
  monitor.result.datapoints_get_retry_count = 0
  monitor.result.datapoints_get_success_count = 0

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
    _id: monitor.result_pre.job_id,
    items: [],
    changes: [],
    stats: {
      started_at: new Date(),
      stations_processed_count: 0
    }
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
  const changes = {}

  for (const station of stations) {
    // let datastream
    let datastreamError
    // HACK: Suppress errors and quiety fail
    // try {
    const datastream = await findDatastream(station._id)
    // } catch (err) {
    //   datastreamError = err.message
    // }

    let datapoint
    let datapointError
    try {
      if (datastream) datapoint = await findDatapoint(datastream._id)
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

    // Construct new log entry for this run
    const checkedAt = new Date()
    const threshold =
      (((monitor &&
        monitor.spec &&
        monitor.spec.options &&
        monitor.spec.options.station_offline_threshold) ||
        (datastream &&
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

    // Prepare running logs
    const lastLog =
      (lastItem &&
        lastItem.log &&
        lastItem.log.filter(
          entry => new Date(entry.checked_at) >= checkedAt - logRetention
        )) ||
      []
    const thisLog = [thisEntry, ...lastLog]
    const lastChangeLog = (lastItem && lastItem.change_log) || []
    const thisChangeLog = [...lastChangeLog]

    // Prime the change log with a recent log entry
    if (!thisChangeLog.length && lastLog.length)
      thisChangeLog.unshift(lastLog[0])

    // Determine if there is a status change
    if (!thisChangeLog.length) {
      thisChangeLog.unshift(thisEntry)
    } else if (thisChangeLog[0].status !== thisEntry.status) {
      // Report the change
      const fromEntry = thisChangeLog[0]
      const { datapoint, status } = thisEntry
      if (!changes[status]) changes[status] = []
      changes[status].push(
        Object.assign(
          {
            station,
            from_status: fromEntry.status,
            from_status_duration: checkedAt - new Date(fromEntry.checked_at)
          },
          fromEntry.datapoint
            ? {
                from_datapoint_duration:
                  checkedAt - new Date(fromEntry.datapoint.t)
              }
            : undefined,
          datapoint ? { datapoint } : undefined
        )
      )

      thisChangeLog.unshift(thisEntry)
    }
    thisChangeLog.splice(2) // Only keep 2 recent change log entries

    // Append report item for this run
    thisReport.items.push({
      change_log: thisChangeLog,
      log: thisLog,
      station
    })

    thisReport.stats.stations_processed_count++
  }

  thisReport.changes = Object.entries(changes).map(([status, items]) => ({
    items,
    to_status: status
  }))

  const finishedAt = new Date()
  thisReport.stats.duration = finishedAt - thisReport.stats.started_at
  thisReport.stats.finished_at = finishedAt

  await minioClient.putObject(
    bucketName,
    objectName,
    JSON.stringify(thisReport)
  )

  if (thisReport.changes.length)
    monitor.result.notification = stationStatusChangeNotification({
      changes: thisReport.changes,
      orgSlug: monitor.result_pre.org_slug
    })

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
