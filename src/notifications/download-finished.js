function md({ download }) {
  return (
    `Dendra download \`${download._id}\` finished.\n\n` +
    'Details\n```\n' +
    (download.spec && download.spec.comment
      ? `Comment: ${download.spec.comment}\n`
      : '') +
    (download.state ? `State: ${download.state}\n` : '') +
    (download.result && download.result.items
      ? `Items: ${download.result.items.length}\n`
      : '') +
    '```\n'
  )
}

function shortText({ download }) {
  return `Dendra download [${download._id}] finished.`
}

function subject() {
  return 'Dendra Download Notification'
}

function text({ download }) {
  return (
    `Dendra download [${download._id}] finished.\n\n` +
    'Details\n' +
    (download.spec && download.spec.comment
      ? `- Comment: ${download.spec.comment}\n`
      : '') +
    (download.state ? `- State: ${download.state}\n` : '') +
    (download.result && typeof download.result.record_count === 'number'
      ? `- Records: ${download.result.record_count}\n`
      : '')
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
