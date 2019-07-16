import * as _ from 'lodash'
import * as MQ from 'amqplib'
import { DelayFunction, ConsumeFunction } from './schema'

const debug = require('debug')('retry:wrapper')

const getDefaultDelay: DelayFunction = attempts => {
  const delay = Math.pow(2, attempts + 2)
  if (delay > 60 * 60 * 24) {
    return -1
  }

  return delay * 1000
}

function parseQueueMsgContent(msgObj: MQ.ConsumeMessage, logger): JSON | undefined {
  try {
    const msgStr = msgObj.content.toString('utf8')
    logger.info('parse mq', msgStr)
    return JSON.parse(msgStr)
  } catch(error) {
    debug('consumeHandler caught error when try to parse msg: ', error)
  }
}

type HandlerWrapperOptions = {
  logger: any
  channel: MQ.ConfirmChannel
  clientQueueName: string
  failureQueueName: string
  clientHandler: Function
  failureHandler: Function
  delayQueueName: string
  delayFunction?: DelayFunction
}

export function handlerWrapper (options: HandlerWrapperOptions): ConsumeFunction {
  const { logger, channel, clientQueueName, failureQueueName, delayQueueName, clientHandler, failureHandler, delayFunction } = options

  const errorHandler = async (msg: MQ.ConsumeMessage, error: Error): Promise<boolean> => {
    _.defaults(msg, { properties: {} })
    _.defaults(msg.properties, { headers: {} })
    _.defaults(msg.properties.headers, { _retryCount: 0 })
    _.defaults(msg.properties.headers, { _errorMsg: (_.isObject(error.message) ? JSON.stringify(error.message) : error.message) })

    msg.properties.headers._retryCount += 1
    debug(`consume message: retry the ${msg.properties.headers._retryCount} time`)
    const expiration = (delayFunction || getDefaultDelay)(msg.properties.headers._retryCount)

    if (expiration < 1) {
      debug(`The retries limit has been exceeded, send message to channel: ${failureQueueName}`)
      await failureHandler(parseQueueMsgContent(msg, logger), error)
      return channel.sendToQueue(failureQueueName, new Buffer(msg.content), msg.properties)
    }

    const properties = {

      /**
       * persistent (boolean): If truthy, the message will survive broker restarts provided 
       * it’s in a queue that also survives restarts. Corresponds to, and overrides, the property
       */
      persistent: true,
      headers: {
        _originalProperties: msg.properties,
        _targetQueue: clientQueueName
      }
    }

    // set expiration time
    _.extend(properties, {
      /**
       * expiration (string): if supplied, the message will be discarded from a queue once 
       * it’s been there longer than the given number of milliseconds. In the specification 
       * this is a string; numbers supplied here will be coerced to strings for transit.
       */
      expiration: expiration.toString()
    })

    debug(`process message failed: send message to delay channel: ${delayQueueName}`)
    return channel.publish('', delayQueueName, new Buffer(msg.content), properties)
  }

  return async msg => {
    if (!msg) {
      return
    }

    let clientError

    try {
      await clientHandler(parseQueueMsgContent(msg, logger))
    } catch(err) {
      // debug('Error: MQ retry handler caught error: ', err.message)
      logger && logger.error('Error: MQ retry handler caught error: ', err.message)
      clientError = err
    } finally {
      // make sure cnsumed message not keep stay in clientQueue
      debug(`channel: ${clientQueueName} message confirmed`)
      channel.ack(msg)
    }

    if (!clientError) {
      return
    }

    try {
      await errorHandler(msg, clientError as Error)
    } catch(err) {
      logger && logger.error('Error: Caught error when MQRetryHandler try to caught client error: ', err.message)
      channel.nack(msg)
      throw err
    }
  }
}

