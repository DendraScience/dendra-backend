const util = require('util')
const exec = util.promisify(require('child_process').exec)
const execFile = util.promisify(require('child_process').execFile)
const spawn = require('child_process').spawn

module.exports = {
  // name: 'service',
  // version: 2,

  /**
   * Settings
   */
  // settings: {},

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
    createSubprocess(options) {
      const subprocess = {
        id: this.nextSubprocessId++,
        ...options
      }
      const { id } = subprocess

      if (this.subprocesses.has(id))
        throw new Error(`Subprocess ${id} already exists.`)

      this.subprocesses.set(id, subprocess)
      return subprocess
    },

    deleteSubprocess(id) {
      return this.subprocesses.delete(id)
    },

    killAll(...args) {
      this.subprocesses.keys().forEach(id => this.killSubprocess(id, ...args))
    },

    killSubprocess(id, ...args) {
      const subprocess = this.subprocesses.get(id)
      const child =
        subprocess &&
        (subprocess.child || (subprocess.promise && subprocess.promise.child))

      if (child && child.exitCode !== null) {
        this.logger.info(`Killing subprocess ${id}.`)
        return child.kill(...args)
      }
    },

    exec(command, options) {
      const subprocess = this.createSubprocess(options)
      const { id, childOptions } = subprocess

      this.logger.info(`Subprocess ${id} is starting for exec: ${command}`)

      subprocess.promise = exec(command, childOptions).finally(() => {
        this.logger.info(`Subprocess ${id} finished.`)
        this.deleteSubprocess(id)
      })

      return subprocess
    },

    execFile(file, args = [], options) {
      const subprocess = this.createSubprocess(options)
      const { id, childOptions } = subprocess

      this.logger.info(`Subprocess ${id} is starting for execFile: ${file}`)

      subprocess.promise = execFile(file, args, childOptions).finally(() => {
        this.logger.info(`Subprocess ${id} finished.`)
        this.deleteSubprocess(id)
      })

      return subprocess
    },

    spawn(command, args = [], options) {
      const subprocess = this.createSubprocess(options)
      const { id, childOptions } = subprocess

      this.logger.info(`Subprocess ${id} is starting for spawn: ${command}`)

      subprocess.promise = new Promise((resolve, reject) => {
        const child = spawn(command, args, childOptions)
        subprocess.child = child
        child.on('close', code => {
          if (code === 0) resolve()
          else
            reject(
              new Error(`Subprocess ${id} returned non-zero exit code ${code}.`)
            )
        })
        child.on('error', reject)
      }).finally(() => {
        if (subprocess.child) subprocess.child.removeAllListeners()
        this.logger.info(`Subprocess ${id} finished.`)
        this.deleteSubprocess(id)
      })

      return subprocess
    }
  },

  /**
   * Service created lifecycle event handler
   */
  created() {
    this.nextSubprocessId = 1
    this.subprocesses = new Map()
  }

  /**
   * Service started lifecycle event handler
   */
  // async started() {},

  /**
   * Service stopped lifecycle event handler
   */
  // async stopped() {}
}
