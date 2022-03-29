/**
 * Datapoints config building.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/dp-config
 */

const { cloneDeep, get } = require('lodash')
const { DateTime, Interval } = require('luxon')

const DATE_TIME_OPTS = {
  zone: 'utc'
}

// Reasonable min and max dates to perform low-level querying
// NOTE: Didn't use min/max integer since db date conversion could choke
// NOTE: Revised to be within InfluxDB default dates
const MAX_TIME = Date.UTC(2200, 1, 2)
const MIN_TIME = Date.UTC(1800, 1, 2)
const MAX_DATE_TIME = DateTime.fromMillis(MAX_TIME, DATE_TIME_OPTS)
const MIN_DATE_TIME = DateTime.fromMillis(MIN_TIME, DATE_TIME_OPTS)

/*
  Wraps an annotation document. Provides useful accessors and methods.
 */
class Annotation {
  constructor(props) {
    Object.assign(this, props)
  }

  get beginsAt() {
    if (!this._beginsAt)
      this._beginsAt = fromISO(this.intervalDoc.begins_at, MIN_DATE_TIME)
    return this._beginsAt
  }

  get endsBefore() {
    if (!this._endsBefore)
      this._endsBefore = fromISO(this.intervalDoc.ends_before, MAX_DATE_TIME)
    return this._endsBefore
  }

  get interval() {
    return Interval.fromDateTimes(this.beginsAt, this.endsBefore)
  }

  hasActions() {
    return this.doc.actions && this.doc.actions.length
  }
}

/*
  Wraps an config instance document. Provides useful accessors and methods.
 */
class ConfigInstance {
  constructor(props) {
    Object.assign(
      this,
      {
        actions: {},
        annotationIds: []
      },
      props
    )
  }

  get beginsAt() {
    if (!this._beginsAt)
      this._beginsAt = fromISO(this.doc.begins_at, MIN_DATE_TIME)
    return this._beginsAt
  }

  set beginsAt(value) {
    this._beginsAt = value
  }

  get beginsAtMillis() {
    return this.beginsAt.toMillis()
  }

  set beginsAtMillis(value) {
    this.beginsAt = DateTime.fromMillis(value, DATE_TIME_OPTS)
  }

  get endsBefore() {
    if (!this._endsBefore)
      this._endsBefore = fromISO(this.doc.ends_before, MAX_DATE_TIME)
    return this._endsBefore
  }

  set endsBefore(value) {
    this._endsBefore = value
  }

  get endsBeforeMillis() {
    return this.endsBefore.toMillis()
  }

  set endsBeforeMillis(value) {
    this.endsBefore = DateTime.fromMillis(value, DATE_TIME_OPTS)
  }

  get interval() {
    return Interval.fromDateTimes(this.beginsAt, this.endsBefore)
  }

  set interval(value) {
    this.beginsAt = value.start
    this.endsBefore = value.end
  }

  applyActions({ doc }) {
    const { actions } = this

    /*
      Check for and apply the 'attrib' actions.
     */

    const attribActions = doc.actions.filter(action => action.attrib)
    if (attribActions.length) {
      const objs = attribActions.map(action => action.attrib)
      actions.attrib = Object.assign({}, actions.attrib, ...objs)
    }

    /*
      Check for and apply the 'evaluate' actions.
     */

    const evaluateActions = doc.actions.filter(action => action.evaluate)
    if (evaluateActions.length) {
      const expr = evaluateActions.map(action => action.evaluate).join(';')
      actions.evaluate = actions.evaluate ? `${actions.evaluate};${expr}` : expr
    }

    /*
      Check for and apply the 'exclude' action.
     */

    const excludeAction = doc.actions.find(action => action.exclude === true)
    if (excludeAction) actions.exclude = true

    /*
      Check for and apply the 'flag' actions.
     */

    const flagActions = doc.actions.filter(action => Array.isArray(action.flag))
    if (flagActions.length) {
      const flag = flagActions.reduce(
        (acc, action) => acc.concat(action.flag),
        []
      )
      actions.flag = actions.flag ? actions.flag.concat(flag) : flag
    }

    // TODO: Add additional actions here

    // Append a reference to the annotation
    this.annotationIds.push(doc._id)

    return this
  }

  cloneWithInterval(interval) {
    return new ConfigInstance({
      actions: cloneDeep(this.actions),
      annotationIds: cloneDeep(this.annotationIds),
      doc: this.doc,
      interval
    })
  }

  mergedDoc(datastream) {
    const { actions, annotationIds, doc, interval } = this

    const newDoc = Object.assign({}, doc, {
      begins_at: interval.start.toISO(),
      ends_before: interval.end.toISO()
    })

    if (Object.keys(actions).length) {
      // Token replace attributes in evaluate expressions
      if (actions.evaluate) {
        const attributes = Object.assign(
          {},
          datastream.attributes,
          actions.attrib
        )
        actions.evaluate = actions.evaluate.replace(/@{([.\w]+)}/g, (m, p) =>
          get(attributes, p, null)
        )
      }

      newDoc.actions = actions
    }
    if (annotationIds.length) newDoc.annotation_ids = annotationIds

    return newDoc
  }
}

function applyAnnotationToConfig(annotation, config) {
  const newConfig = []

  for (const inst of config) {
    if (!annotation.interval.overlaps(inst.interval)) {
      newConfig.push(inst)
      continue
    }

    // Append non-overlaping instances
    inst.interval.difference(annotation.interval).forEach(interval => {
      newConfig.push(inst.cloneWithInterval(interval))
    })

    // Append intersecting instance
    const intersect = inst.interval.intersection(annotation.interval)
    if (intersect)
      newConfig.push(inst.cloneWithInterval(intersect).applyActions(annotation))
  }

  return newConfig
}

function configSortPredicate(a, b) {
  if (a.beginsAtMillis < b.beginsAtMillis) return -1
  if (a.beginsAtMillis > b.beginsAtMillis) return 1
  return 0
}

function createRefs(config) {
  const refd = []

  for (const inst of config) {
    const { connection, params, path } = inst

    delete inst.connection
    delete inst.params
    delete inst.path
    inst.ref = refd.length

    refd.push(
      Object.assign(
        {},
        connection ? { connection } : undefined,
        params ? { params } : undefined,
        path ? { path } : undefined
      )
    )
  }

  return { config, refd }
}

function fromISO(iso, invalid) {
  const dateTime = DateTime.fromISO(iso, DATE_TIME_OPTS)
  return dateTime.isValid ? dateTime : invalid
}

function preprocessConfig(config) {
  const stack = []

  // Efficiently merge config instances in a linear traversal
  config
    .map(
      doc =>
        new ConfigInstance({
          // Include initial actions, e.g. evaluate
          actions: Object.assign({}, doc.actions),
          doc
        })
    )
    .sort(configSortPredicate)
    .forEach(inst => {
      if (inst.endsBeforeMillis <= inst.beginsAtMillis) {
        // Exclude: inverted interval
      } else if (stack.length === 0) {
        stack.push(inst) // Init stack
      } else {
        const top = stack[stack.length - 1]

        if (inst.beginsAtMillis >= top.endsBeforeMillis) {
          stack.push(inst)
        } else if (inst.endsBeforeMillis <= top.endsBeforeMillis) {
          // Exclude: instance interval is within top interval
        } else if (inst.beginsAtMillis === top.beginsAtMillis) {
          stack.pop()
          stack.push(inst)
        } else {
          top.endsBeforeMillis = inst.beginsAtMillis
          stack.push(inst)
        }
      }
    })

  return stack
}

module.exports = {
  DATE_TIME_OPTS,
  MAX_DATE_TIME,
  MIN_DATE_TIME,
  MAX_TIME,
  MIN_TIME,
  Annotation,
  ConfigInstance,
  applyAnnotationToConfig,
  configSortPredicate,
  createRefs,
  fromISO,
  preprocessConfig
}
