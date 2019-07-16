import * as MQ from 'amqplib'
import { mqRetry } from './retry'
import { DelayFunction } from './schema'

const debug = require('debug')('retry: queue')

export interface QueueConfig {
  ex: string
  exType: string,
  durable: boolean,
  routeKey: string,
  autoDelete: boolean,
  queue: string
  failQueue: string
}

export interface QueueParams<T, P> {
  clients: any
  delayFunction: DelayFunction
  context: T
  logger: P
  moduleName: string
  funcName: string
  consumeHandler: Function
  failureHandler: Function
  [key: string]: any
}

const defaultQueueParams = {

}

interface consumeOptions {
  prefetchCount?: number
}

interface RetryOptions {
  callbackOptions?: object
  checkIsOk?: Function
  onMaxFail?: Function
  delay?: number
  max?: number
}

const sleep = (time: number) => {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve()
    }, time)
  })
}

async function retry(caller: any, callback: Function, retryOptions: RetryOptions = {}, count = 0) {
  let { 
    callbackOptions,
    checkIsOk,
    onMaxFail,
    delay = 0,
    max = 2
  } = retryOptions

  const logger = (caller && caller.logger) ||  {
    error(msg) {
      console.log(msg)
    }
  }

  if (!checkIsOk) {
    checkIsOk = () => {
      return true
    }
  }

  if (!onMaxFail) {
    onMaxFail = () => {
      logger.error(`retry failed: the ${max} time`)
    }
  }

  if (count > max) {
    onMaxFail()
    return null
  }

  if (count > 0) {
    logger.error(`retry: the ${count} time`)
  }

  try {
    const result = await callback.call(caller, callbackOptions)

    if (checkIsOk.call(caller, result)) {
      return result
    }

    throw new Error('retry result not satisfied')
  } catch(e) {
    if (delay) {
      await sleep(delay)
    }
    count = count + 1
    logger.error(e)
    await retry(caller, callback, retryOptions, count)
  }
}

export class Queue<T, P> {
  params: QueueParams<T, P>
  config: QueueConfig
  logKey: string
  connector: MQ.Connection | null
  channel: MQ.ConfirmChannel | null
  clients: Array<MQ.Options.Connect>
  currentClientIndex: number
  delayFunction?: DelayFunction

  constructor(params: QueueParams<T, P>) {
    this.params = {
      ...defaultQueueParams,
      ...params
    }
    this.config = this.buildConfig(params.moduleName, params.funcName)
    this.logKey = `rabbitmq: ${this.config.queue}`
    this.chooseServer()

    return this
  }

  static async create<T, P>(params: QueueParams<T, P>): Promise<Queue<T, P>> {
    const queueInstance = new Queue(params)

    await queueInstance.connect()
    queueInstance.consume()
    return queueInstance
  }

  chooseServer() {
    this.clients = this.params.clients
    this.delayFunction = this.params.delayFunction
    this.currentClientIndex = 0
  }

  switchServer() {
    if (this.currentClientIndex === (this.clients.length - 1)) {
      this.currentClientIndex = 0
    } else {
      this.currentClientIndex++
    }

    debug(`try to switch another mq server: ${this.clients[this.currentClientIndex].hostname}`)
  }

  async connect(): Promise<Queue<T, P> | undefined> {
    const config = this.config
    const logKey = this.logKey

    try {
      const currentClient = this.clients[this.currentClientIndex]

      debug(`try to connect mq server: ${currentClient.hostname}`)

      const connector = await MQ.connect(currentClient)
      this.connector = connector

      const channel = await connector.createConfirmChannel()
      await channel.assertExchange(config.ex, config.exType, { durable: config.durable, autoDelete: config.autoDelete })
      await channel.assertQueue(config.queue, { exclusive: false, durable: config.durable, autoDelete: config.autoDelete })
      await channel.assertQueue(config.failQueue, { exclusive: false, durable: config.durable, autoDelete: config.autoDelete })
      await channel.bindQueue(config.queue, config.ex, config.routeKey)

      debug(`${logKey}: create queue successfully`)
      this.channel = channel

      this.listen(this.connector)

    } catch (e) {
      debug(`${logKey}: create queue failed`, e)
    }

    return this
  }


  listen(connector: MQ.Connection) {
    connector.on('error', this.onConnError)
    connector.on('close', this.onConnClosed)
  }

  onConnError = (err: Error) => {
    debug(`${this.logKey}: connect error: `, err)
  }

  onConnClosed = async (err: Error) => {
    const { context } = this.params
    debug(`${this.logKey}: connect closed: `, err)

    retry(context, this.retryOnClose, {
      max: 3000,
      delay: 30 * 1000,
      checkIsOk: (channel) => {
        return channel
      }
    })
  }

  retryOnClose = async () => {
    this.connector = null
    this.channel = null

    this.switchServer()

    await this.connect()

    this.consume()

    return this.channel
  }

  buildConfig(moduleName, funcName): QueueConfig {
    const ex = `${moduleName}.${funcName}`
    return {
      ex,
      exType: 'direct',
      durable: true,
      routeKey: ex,
      autoDelete: false,
      queue: `${ex}.queue`,
      failQueue: `${ex}.queue.failure`
    }
  }

  async produce(msg): Promise<any> {
    const { consumeHandler, failureHandler } = this.params
    debug('call produce -------')
    const { ex, routeKey } = this.config

    const channel = this.channel
    if (!channel) {
      debug(`no channel found when call produce: ${this.config.queue}`)
      return
    }

    const tailParams = {
      persistent: true, // for message persistence
      mandatory: true   // will return to producer when suitable key not found
    }

    let isPublishError = false

    try {
      const msgStr = JSON.stringify(msg)
      const result = await channel.publish(ex, routeKey, Buffer.from(msgStr), tailParams)
      return result
    } catch(err) {
      debug(`${this.logKey}: publish failed`, err)
      isPublishError = true
    }

    if (isPublishError) {
      try {
        debug(`${this.logKey}: publish failed, excute consumeHandler immediately`)
        await consumeHandler(msg)
      } catch(err) {
        debug(`${this.logKey}: publish failed, excute consumeHandler failed, try to excute failureHandler`)
        await failureHandler(msg)
      }
    }
  }

  async consume(options: consumeOptions = {}) {
    const channel = this.channel

    if (!channel) {
      debug(`no channel found when call consume: ${this.config.queue}`)
      return
    }

    if (options.prefetchCount) {
      await channel.prefetch(options.prefetchCount)
    }

    const { consumeHandler, failureHandler, logger } = this.params

    // wrap origin handler for automic retry
    const handler = await mqRetry({
      logger,
      channel,
      consumeHandler,
      failureHandler,
      consumerQueue: this.config.queue,
      failureQueue: this.config.failQueue,
      delayFunction: this.delayFunction
    })

    channel.consume(this.config.queue, handler)
  }
}