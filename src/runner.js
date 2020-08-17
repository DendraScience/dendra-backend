// #!/usr/bin/env node --expose-gc

/* moleculer
 * Copyright (c) 2019 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

// 'use strict'

if (global.gc) {
  /* eslint-disable-next-line no-console */
  console.log('GC!!!')
}

const { Runner } = require('moleculer')

const runner = new Runner()
runner.start(process.argv)
