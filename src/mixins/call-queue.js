module.exports = {
  // name: 'service',
  // version: 2,

  /**
   * Settings
   */
  settings: {
    maxActiveCalls: 1
  },

  /**
   * Dependencies
   */
  // dependencies: [],

  /**
   * Actions
   */
  // actions: {},

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  methods: {
    ackCall(id) {
      const call = this.activeCalls.get(id)
      if (call) {
        this.logger.info(`Call ${id} acked.`)
        call.acked = true
        return call.finished && this.deleteCall(id)
      }
    },

    deleteCall(id) {
      const deleted = this.activeCalls.delete(id)
      if (deleted) this.nextDispatch()
      return deleted
    },

    finishCall(id) {
      const call = this.activeCalls.get(id)
      if (call) {
        this.logger.info(`Call ${id} finished.`)
        call.finished = true
        return call.acked && this.deleteCall(id)
      }
    },

    queueCall(call) {
      const { id } = call

      if (this.activeCalls.has(id))
        throw new Error(`Active call ${id} already exists.`)
      if (this.queuedCalls.find(call => call.id === id))
        throw new Error(`Queued call ${id} already exists.`)

      this.queuedCalls.unshift(call)
      this.nextDispatch()
      return call
    },

    queueMethod(method, args = [], options) {
      if (typeof this[method] !== 'function')
        throw new Error(`Invalid method name '${method}'.`)

      const call = this.queueCall({
        acked: true,
        id: this.nextCallId++,
        ...options,
        method,
        args,
        finished: false
      })
      this.logger.info(`Call ${call.id} queued for method: ${method}`)
      return call
    },

    cancelDispatch() {
      this.dispatchTimer && clearTimeout(this.dispatchTimer)
    },

    nextDispatch() {
      this.dispatchTimer.refresh()
    },

    setDispatchEnabled(value) {
      this.dispatchEnabled = value
      if (value) this.nextDispatch()
    }
  },

  /**
   * Service created lifecycle event handler
   */
  created() {
    this.dispatchEnabled = true
    this.nextCallId = 1
    this.activeCalls = new Map()
    this.queuedCalls = []
  },

  /**
   * Service started lifecycle event handler
   */
  async started() {
    this.dispatchTimer = setTimeout(() => {
      this.logger.info('Dispatch is starting.')

      if (
        !this.dispatchEnabled ||
        this.activeCalls.size >= this.settings.maxActiveCalls ||
        this.queuedCalls.length === 0
      ) {
        this.logger.info('Dispatch deferred.')
        return
      }

      // Move from queued to active
      const call = this.queuedCalls.pop()
      const { id, method, args } = call
      this.activeCalls.set(id, call)

      this.logger.info(`Call ${id} is starting.`)

      call.promise = Promise.resolve(this[method](id, ...args))
        .catch(err => {
          this.ackCall(id)
          throw err
        })
        .finally(() => this.finishCall(id))
    }, 200)
  },

  /**
   * Service stopped lifecycle event handler
   */
  async stopped() {
    this.setDispatchEnabled(false)
    this.cancelDispatch()

    await Promise.all([...this.activeCalls.values()].map(call => call.promise))
  }
}
