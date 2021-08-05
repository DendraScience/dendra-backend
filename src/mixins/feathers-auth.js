const axios = require('axios')
const qs = require('qs')
const { httpAgent, httpsAgent } = require('../lib/http-agent')
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
      const { data } = await this.api.post(
        '/authentication',
        this.settings.feathers.auth
      )

      return data
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
    this.api = axios.create({
      baseURL: this.settings.feathers.url,
      httpAgent,
      httpsAgent,
      maxRedirects: 0,
      paramsSerializer: function (params) {
        return qs.stringify(params)
      },
      timeout: 90000
    })
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
