/**
 * Result patching from scripts.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/result-patcher
 */

class ResultPatcher {
  constructor(options) {
    Object.assign(
      this,
      {
        interval: 50000,
        maxRetryCount: 3,
        maxRetryDelay: 3000
      },
      options,
      {
        pending: Promise.resolve()
      }
    )
  }

  async _patch() {
    let count = 0

    while (true) {
      this.logger.info(`Patching results: ${this.url}`)

      try {
        const resp = await this.webAPI.patch(this.url, {
          $set: {
            result: this.result,
            state: 'running'
          }
        })
        if (resp.data && resp.data.is_cancel_requested === true) {
          this.logger.error(`Cancel requested. Exiting script.`)
          process.exit(2) // Fatal
        }
        break
      } catch (err) {
        if (count++ >= this.maxRetryCount || err.code !== 'ECONNABORTED') {
          this.logger.error(`ResultPatcher error: ${err.message}`)
          process.exit(1) // Fatal
        }

        await new Promise(resolve => setTimeout(resolve, this.maxRetryDelay))
      }
    }
  }

  patch() {
    return (this.pending = this.pending.then(() => this._patch()))
  }

  start(options) {
    Object.assign(this, options)

    this.timer = setTimeout(() => {
      this._patch().then(() => {
        if (this.timer) this.timer.refresh()
      })
    }, this.interval)
  }

  stop() {
    if (this.timer) {
      clearTimeout(this.timer)
      delete this.timer
    }
  }
}

module.exports = {
  ResultPatcher
}
