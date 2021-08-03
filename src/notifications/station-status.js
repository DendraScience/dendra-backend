const statusEmoji = {
  error: ':ladybug:',
  offline: ':heavy_exclamation_mark:',
  online: ':white_check_mark:',
  unknown: ':grey_question:'
}
const statuses = ['offline', 'online', 'unknown', 'error']
const webSiteUrl = process.env.WEB_SITE_URL || 'https://dendra.science'
const { sortBy } = require('lodash')

function changesOrdered(changes) {
  return statuses
    .map(status => changes.find(change => change.to_status === status))
    .filter(change => !!change)
}

function formatLastSeen({ datapoint, station }) {
  return station.time_zone && datapoint && datapoint.lt
    ? datapoint.lt.substring(0, 10) +
        ' ' +
        datapoint.lt.substring(11, 16) +
        ' ' +
        station.time_zone
    : datapoint && datapoint.t
    ? datapoint.t.substring(0, 10) +
      ' ' +
      datapoint.t.substring(11, 16) +
      ' UTC'
    : ''
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
          items
            .map(item => {
              const lastSeen = formatLastSeen(item)
              return (
                item.station.name +
                (change.to_status !== 'online' && lastSeen
                  ? ` (last seen ${lastSeen})`
                  : '')
              )
            })
            .join('\n') +
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
            .map(item => {
              const lastSeen = formatLastSeen(item)
              return (
                '- ' +
                item.station.name +
                (change.to_status !== 'online' && lastSeen
                  ? ` (last seen ${lastSeen})`
                  : '')
              )
            })
            .join('\n') +
          '\n'
        )
      })
      .join('\n') +
    '\n'
  )
}

function createNotification(data) {
  return {
    md: md(data),
    short_text: shortText(data),
    subject: subject(data),
    text: text(data)
  }
}

module.exports = {
  createNotification
}
