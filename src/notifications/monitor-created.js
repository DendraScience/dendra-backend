const { contactEmail } = require('./consts')
const testStatement =
  'Congratulations! If you are reading this, then it means you can receive alerts for this new monitoring configuration. If you wish to stop receiving alerts, email us at ' +
  contactEmail
const testStatementShort =
  'This is a test. To opt out, email us at ' + contactEmail

function md({ monitor }) {
  return (
    `Dendra monitor \`${monitor._id}\` created.\n\n` +
    'Details\n```\n' +
    (monitor.spec && monitor.spec.comment
      ? `Comment: ${monitor.spec.comment}\n`
      : '') +
    '```\n\n' +
    testStatement +
    '\n'
  )
}

function shortText({ monitor }) {
  return `Dendra monitor [${monitor._id}] created. ${testStatementShort}`
}

function subject() {
  return 'Dendra Monitor Notification'
}

function text({ monitor }) {
  return (
    `Dendra monitor [${monitor._id}] created.\n\n` +
    'Details\n' +
    (monitor.spec && monitor.spec.comment
      ? `- Comment: ${monitor.spec.comment}\n`
      : '') +
    '\n\n' +
    testStatement +
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
