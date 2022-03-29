/**
 * Child process script.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module datapoints-config/build
 */

const path = require('path')
const logger = require('pino')({
  level: process.env.LOGLEVEL,
  name: path.basename(process.argv[1], '.js')
})
const {
  createWebAPI,
  setupProcessHandlers
} = require('../../lib/script-helpers')
setupProcessHandlers(process, logger)

const datastreamId = process.argv[2]
const {
  Annotation,
  applyAnnotationToConfig,
  configSortPredicate,
  createRefs,
  preprocessConfig
} = require('../../lib/dp-config')

logger.info('Script is starting.')

const webAPI = createWebAPI({
  accessToken: process.env.WEB_API_ACCESS_TOKEN,
  baseURL: process.env.WEB_API_URL
})

async function run() {
  const datastreamResp = await webAPI.get(`/datastreams/${datastreamId}`)
  const datastream = datastreamResp.data

  if (datastream.source_type !== 'sensor')
    throw new Error('Datastream source_type must be sensor.')

  const annotationsResp = await webAPI.get('/annotations', {
    params: {
      is_enabled: true,
      state: 'approved',
      $or: [
        {
          station_ids: datastream.station_id
        },
        {
          datastream_ids: datastream._id
        }
      ],
      $limit: 2000, // FIX: Implement unbounded find or pagination
      $sort: {
        _id: 1 // ASC
      }
    }
  })
  const annotations = (
    (annotationsResp.data && annotationsResp.data.data) ||
    []
  )
    .map(doc => {
      return doc.intervals
        ? doc.intervals.map(intervalDoc => new Annotation({ doc, intervalDoc }))
        : [new Annotation({ doc, intervalDoc: {} })]
    })
    .reduce((acc, cur) => acc.concat(cur), [])

  logger.info(`Processing (${annotations.length}) annotation intervals.`)

  /*
    Update the datapoints config based on each annotation.
   */

  let { config, refd } = createRefs(datastream.datapoints_config || [])
  config = preprocessConfig(config)

  for (const annotation of annotations) {
    if (annotation.hasActions()) {
      config = applyAnnotationToConfig(annotation, config)
      config = config.sort(configSortPredicate)
    }
  }

  config = config.map(inst => inst.mergedDoc(datastream))

  /*
    Patch the datastream with the built config.
   */

  logger.info(`Patching datastream ${datastream._id}.`)

  await webAPI.patch(
    `/datastreams/${datastreamId}`,
    {
      $set: {
        datapoints_config_built: config,
        datapoints_config_refd: refd
      }
    },
    {
      params: {
        source_type: 'sensor'
      }
    }
  )
}

run()
  .catch(err => {
    logger.error(`Run error: ${err.message}`)
    process.exit(1)
  })
  .finally(() => {
    logger.info('Script finished.')
  })
