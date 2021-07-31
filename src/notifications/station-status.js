const statuses = ['offline', 'online', 'unknown', 'error']
const webSiteUrl = process.env.WEB_SITE_URL || 'https://dendra.science'
const { sortBy } = require('lodash')

function changesOrdered(changes) {
  return statuses
    .map(status => changes.find(change => change.to_status === status))
    .filter(change => !!change)
}

function formatLastSeen(item) {
  return item.station.time_zone && item.datapoint && item.datapoint.lt
    ? item.datapoint.lt.substring(0, 10) +
        item.datapoint.lt.substring(11, 16) +
        ' ' +
        item.station.time_zone
    : item.datapoint && item.datapoint.t
    ? item.datapoint.t.substring(0, 10) +
      item.datapoint.t.substring(11, 16) +
      ' UTC'
    : ''
}

function md({ changes, orgSlug }) {
  // TODO: Finish this!
  return ''
}

function shortText({ changes, orgSlug }) {
  return 'Dendra station status change. Stations ' +
    changes
      .map(change => `${change.to_status} (${change.items.length})`)
      .join(', ') +
    '.' +
    orgSlug
    ? `Visit ${webSiteUrl}/orgs/${orgSlug}/status`
    : `Visit ${webSiteUrl}`
}

function subject({ orgSlug }) {
  return 'Dendra Station Status Alert' + orgSlug ? ` [${orgSlug}]` : ''
}

function text({ changes, orgSlug }) {
  return 'Dendra monitoring detected a station status change' + orgSlug
    ? ` for the org ${orgSlug}. For details visit ${webSiteUrl}/orgs/${orgSlug}/status`
    : `. For details visit ${webSiteUrl}\n\n` +
        changesOrdered(changes)
          .map(change => {
            const items = sortBy(change.items, ['station.name'])
            return (
              `Stations have changed to ${change.to_status}:\n` +
              items
                .map(item => {
                  const lastSeen = formatLastSeen(item)
                  return `- ${item.station.name}` +
                    (change.to_status === 'offline' && lastSeen)
                    ? ` (last seen ${lastSeen})`
                    : ''
                })
                .join('\n')
            )
          })
          .join('\n\n') +
        '\n'
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
