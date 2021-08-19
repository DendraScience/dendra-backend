function md({ upload }) {
  return (
    `Dendra upload \`${upload._id}\` finished.\n\n` +
    'Details\n```\n' +
    (upload.spec && upload.spec.comment
      ? `Comment: ${upload.spec.comment}\n`
      : '') +
    (upload.state ? `State: ${upload.state}\n` : '') +
    (upload.result && upload.result.items
      ? `Items: ${upload.result.items.length}\n`
      : '') +
    '```\n'
  )
}

function shortText({ upload }) {
  return `Dendra upload [${upload._id}] finished.`
}

function subject() {
  return 'Dendra Upload Notification'
}

function text({ upload }) {
  return (
    `Dendra upload [${upload._id}] finished.\n\n` +
    'Details\n' +
    (upload.spec && upload.spec.comment
      ? `- Comment: ${upload.spec.comment}\n`
      : '') +
    (upload.state ? `- State: ${upload.state}\n` : '') +
    (upload.result && Array.isArray(upload.result.items)
      ? `- Items: ${upload.result.items.length}\n`
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
