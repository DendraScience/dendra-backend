const moment = require('moment')
const { sortBy } = require('lodash')
const { webSiteUrl } = require('./consts')
const lastSeenFormat = 'YYYY-MM-DD HH:mm'
const statusEmoji = {
  error: ':ladybug:',
  offline: ':heavy_exclamation_mark:',
  online: ':white_check_mark:',
  unknown: ':grey_question:'
}
const statuses = ['offline', 'online', 'unknown', 'error']

function changesOrdered(changes) {
  return statuses
    .map(status => changes.find(change => change.to_status === status))
    .filter(change => !!change)
}

function formatLastSeen({ datapoint, station }) {
  return station.time_zone && datapoint && datapoint.lt
    ? moment.utc(datapoint.lt).format(lastSeenFormat) + ' ' + station.time_zone
    : datapoint && datapoint.t
    ? moment.utc(datapoint.t).format(lastSeenFormat) + ' UTC'
    : ''
}

function formatStationLine({ change, item }) {
  let suffix = ''
  if (change.to_status === 'online') {
    if (item.from_status && item.from_status_duration)
      suffix = ` (was ${item.from_status} for ${moment
        .duration(item.from_status_duration, 'ms')
        .humanize()})`
  } else {
    const lastSeen = formatLastSeen(item)
    if (lastSeen) suffix = ` (last seen ${lastSeen})`
  }
  return item.station.name + suffix
}

function md({ changes, orgSlug }) {
  return (
    'Dendra monitoring detected a station status change' +
    (orgSlug
      ? ` for the org \`${orgSlug}\`. For details visit ${webSiteUrl}/orgs/${orgSlug}/status\n\n`
      : `. For details visit ${webSiteUrl}\n\n`) +
    changesOrdered(changes)
      .map(change => {
        // TODO: Trim items, max 20 or so?
        const items = sortBy(change.items, ['station.name'])
        const emoji = statusEmoji[change.to_status] || ':neutral_face:'
        return (
          `${emoji} Stations have changed to \`${change.to_status}\`\n\`\`\`\n` +
          items.map(item => formatStationLine({ change, item })).join('\n') +
          '\n```\n'
        )
      })
      .join('\n') +
    '\n'
  )
}

function shortText({ changes, orgSlug }) {
  return (
    'Dendra station status change. Stations ' +
    changes
      .map(change => `${change.to_status} (${change.items.length})`)
      .join(', ') +
    '. ' +
    (orgSlug
      ? `Visit ${webSiteUrl}/orgs/${orgSlug}/status`
      : `Visit ${webSiteUrl}`)
  )
}

function subject({ orgSlug }) {
  return 'Dendra Station Status Alert' + (orgSlug ? ` [${orgSlug}]` : '')
}

function text({ changes, orgSlug }) {
  return (
    'Dendra monitoring detected a station status change' +
    (orgSlug
      ? ` for the org [${orgSlug}]. For details visit ${webSiteUrl}/orgs/${orgSlug}/status\n\n`
      : `. For details visit ${webSiteUrl}\n\n`) +
    changesOrdered(changes)
      .map(change => {
        // TODO: Trim items, max 20 or so?
        const items = sortBy(change.items, ['station.name'])
        return (
          `Stations have changed to [${change.to_status}]\n` +
          items
            .map(item => '- ' + formatStationLine({ change, item }))
            .join('\n') +
          '\n'
        )
      })
      .join('\n') +
    '\n'
  )
}

module.exports = data => {
  return {
    md: md(data),
    short_text: shortText(data),
    subject: subject(data),
    text: text(data)
  }
}
