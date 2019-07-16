import * as MQ from 'amqplib'
import { handlerWrapper } from './wrapper'
import { ConsumeFunction, DelayFunction } from './schema'

const debug = require('debug')('mq-retry:retry')

const RETRY_PREFIX = 'mq.retry'
const EXCHANGE_NAME = RETRY_PREFIX
const DELAYED_QUEUE_NAME = RETRY_PREFIX + '.delayed'
const READY_QUEUE_NAME = RETRY_PREFIX + '.ready'
const READY_ROUTE_KEY = 'ready'

const defaultConfig = {
  exchangeName: EXCHANGE_NAME,
  delayQueueName: DELAYED_QUEUE_NAME,
  readyQueueName: READY_QUEUE_NAME,
  readyRouteKey: READY_ROUTE_KEY
}

async function setupRetry(channel: MQ.ConfirmChannel, clientQueueName: string, failureQueueName: string) {
  try {
    await Promise.all([
      channel.assertQueue(defaultConfig.delayQueueName, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': defaultConfig.exchangeName,
          'x-dead-letter-routing-key': defaultConfig.readyRouteKey
        }
      }),
      channel.assertQueue(defaultConfig.readyQueueName, { durable: true }),
      channel.checkQueue(clientQueueName),
      channel.checkQueue(failureQueueName),
      channel.assertExchange(defaultConfig.exchangeName, 'direct', { durable: true }),

      channel.bindQueue(defaultConfig.readyQueueName, defaultConfig.exchangeName, defaultConfig.readyRouteKey)
    ])
   } catch(err) {
     console.log(err)
   }
}

function startConsume(channel: MQ.ConfirmChannel) {
  const handler: ConsumeFunction = async msg => {
    if (!msg) {
      return
    }

    debug(`${defaultConfig.readyQueueName} received message`)
    try {
      const targetQueueName = msg.properties.headers._targetQueue
      const properties = msg.properties.headers._originalProperties
      debug(`${defaultConfig.readyQueueName} send message to target: ${targetQueueName}`)
      await channel.sendToQueue(targetQueueName, new Buffer(msg.content), properties)
      channel.ack(msg)
      debug(`${defaultConfig.readyQueueName} message acked`)
    } catch(err) {
      debug(`${defaultConfig.readyQueueName} message refused. err: ${err}, msg: ${JSON.stringify(msg)}`)
      channel.nack(msg)
    }
  }

  channel.consume(defaultConfig.readyQueueName, handler)
}

export type RetryOptions = {
  logger: any
  channel: MQ.ConfirmChannel
  consumerQueue: string
  failureQueue: string
  consumeHandler: Function
  failureHandler: Function
  delayFunction?: DelayFunction
}

export async function mqRetry(options: RetryOptions) {
  const { logger } = options
  logger && logger.info('begin bind compensate queue')
  await setupRetry(options.channel, options.consumerQueue, options.failureQueue)
  logger && logger.info('end bind compensate queue')
  logger && logger.info('compensate queue begin to listen messages')
  startConsume(options.channel)
  return handlerWrapper({
    logger,
    channel: options.channel,
    clientHandler: options.consumeHandler,
    clientQueueName: options.consumerQueue,
    failureQueueName: options.failureQueue,
    failureHandler: options.failureHandler,
    delayFunction: options.delayFunction,
    delayQueueName: defaultConfig.delayQueueName,
  }) 
}