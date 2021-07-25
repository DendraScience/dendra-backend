const got = require('got')
const jwtDecode = require('jwt-decode')

module.exports = {
  // name: 'service',

  /**
   * Settings
   */
  settings: {
    $secureSettings: ['feathers.auth']
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
    async authenticate() {
      const { body } = await got('authentication', {
        json: this.settings.feathers.auth,
        method: 'POST',
        prefixUrl: this.settings.feathers.url,
        responseType: 'json'
      })

      return body
    },

    async getAuthUser() {
      let { accessToken } = this
      let user

      if (accessToken) {
        const { userId } = jwtDecode(accessToken)
        try {
          user = await this.broker.call(
            'users.get',
            {
              id: userId
            },
            {
              meta: { accessToken }
            }
          )
        } catch (err) {
          this.logger.warn(
            `Get user error (token probably invalid): ${err.message}`
          )
        }
      }

      if (!user) {
        this.accessToken = accessToken = (await this.authenticate()).accessToken
        const { userId } = jwtDecode(accessToken)
        user = await this.broker.call(
          'users.get',
          {
            id: userId
          },
          {
            meta: { accessToken }
          }
        )
      }

      return {
        accessToken,
        user
      }
    }
  },

  /**
   * Service created lifecycle event handler
   */
  created() {
    this.accessToken = null
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
