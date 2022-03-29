const axios = require('axios')
const qs = require('qs')
const { httpAgent, httpsAgent } = require('../lib/http-agent')
const { Readable } = require('stream')

module.exports = {
  // name: 'service',

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
  actions: {
    async create(ctx) {
      const { data } = await this.api.post(`/${this.name}`, {
        headers: this.makeHeaders(ctx.meta),
        params: ctx.params.query
      })

      return data
    },

    async find(ctx) {
      const { data } = await this.api.get(`/${this.name}`, {
        headers: this.makeHeaders(ctx.meta),
        params: ctx.params.query
      })

      return Readable.from(data.data)
    },

    async findByIds(ctx) {
      const ids = ctx.params.ids || []
      const query = Object.assign({}, ctx.params.query, {
        _id: { $in: ids }
      })
      const { data } = await this.api.get(`/${this.name}`, {
        headers: this.makeHeaders(ctx.meta),
        params: query
      })
      const itemsById = data.data.reduce((obj, item) => {
        obj[item._id] = item
        return obj
      }, {})

      return Readable.from(ids.map(id => itemsById[id]))
    },

    async findIds(ctx) {
      const query = Object.assign({}, ctx.params.query, {
        $limit: 1000,
        $select: ['_id'],
        $sort: { _id: 1 }
      })
      const ids = []
      let done

      while (!done) {
        const { data } = await this.api.get(`/${this.name}`, {
          headers: this.makeHeaders(ctx.meta),
          params: query
        })

        let id
        for (let i = 0; i < data.data.length; i++) {
          id = data.data[i]._id
          ids.push(id)
        }
        if (id) query._id = { $gt: id }
        else done = true
      }

      return ids
    },

    async findOne(ctx) {
      const { data } = await this.api.get(`/${this.name}`, {
        headers: this.makeHeaders(ctx.meta),
        params: Object.assign({}, ctx.params.query, {
          $limit: 1
        })
      })

      return data.data && data.data[0]
    },

    async get(ctx) {
      if (!ctx.params.id) throw new Error("id for 'get' can not be undefined")

      const { data } = await this.api.get(`/${this.name}/${ctx.params.id}`, {
        headers: this.makeHeaders(ctx.meta),
        params: ctx.params.query
      })

      return data
    },

    async patch(ctx) {
      const { data } = await this.api.patch(
        ctx.params.id ? `/${this.name}/${ctx.params.id}` : `/${this.name}`,
        ctx.params.data,
        {
          headers: this.makeHeaders(ctx.meta),
          params: ctx.params.query
        }
      )

      return data
    },

    async update(ctx) {
      if (!ctx.params.id)
        throw new Error("id for 'update' can not be undefined")

      const { data } = await this.api.put(
        `/${this.name}/${ctx.params.id}`,
        ctx.params.data,
        {
          headers: this.makeHeaders(ctx.meta),
          params: ctx.params.query
        }
      )

      return data
    }
  },

  /**
   * Events
   */
  // events: {},

  /**
   * Methods
   */
  methods: {
    makeHeaders(meta) {
      const headers = {}
      if (meta.accessToken) headers.Authorization = meta.accessToken

      return headers
    }
  },

  /**
   * Service created lifecycle event handler
   */
  created() {
    this.api = axios.create({
      baseURL: this.settings.url,
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
