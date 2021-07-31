/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

module.exports = {
  name: 'reports',

  /**
   * Actions
   */
  actions: {
    create: {
      params: {
        _id: { type: 'string' }
      },
      rest: 'POST /:_id',
      async handler(ctx) {
        // Put bulky reports in object store
        await ctx.call('minio.putObject', JSON.stringify(ctx.params), {
          meta: {
            bucketName: this.name,
            objectName: `${ctx.params._id}.json`
          }
        })
      }
    }
  }
}
