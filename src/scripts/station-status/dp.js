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

  const response = await webAPI.get('/datastreams', {
    params: Object.assign({}, query, {
      station_id: stationId,
      $limit: 1,
      $select: ['_id', 'general_config', 'general_config_resolved', 'name'],
      $sort: { _id: 1 }
    })
  })

  return (
    (response.data &&
      response.data.data &&
      response.data.data.length &&
      response.data.data[0]) ||
    null
  )
}

async function run() {
  const response = await webAPI.get(`/monitors/${monitorId}`)
  monitor = response.data

  if (!monitor.result_pre) throw new Error('Missing result_pre.')
  if (!monitor.result) monitor.result = {}
  // download.result.datapoints_count = 0
  // download.result.datapoints_get_error_count = 0
  // download.result.datapoints_get_retry_count = 0
  // download.result.datapoints_get_success_count = 0
  // download.result.record_count = 0

  resultPatcher.start({
    result: monitor.result,
    url: `monitors/${monitor._id}`
  })

  const bucketName = monitor.result_pre.bucket_name
  const objectName = monitor.result_pre.object_name

  if (!bucketName) throw new Error('Missing bucket_name.')
  if (!objectName) throw new Error('Missing object_name.')

  let lastReport = null
  try {
    lastReport = JSON.parse(
      await getStream(
        await minioClient.getObject(bucketName, `${objectName}.json`)
      )
    )
  } catch (_) {}

  const thisReport = {
    created_at: new Date(),
    items: []
  }
  const stations = await findStations()

  for (const station of stations) {
    const datastream = await findDatastream(station._id)

    thisReport.items.push({
      station,
      datastream
    })
  }

  /* eslint-disable-next-line no-console */
  console.log('>>>', lastReport, thisReport)

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
