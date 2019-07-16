import * as MQ from 'amqplib'

export type DelayFunction = {
  (num: number): number
}

export type ConsumeFunction = {
  (msg: MQ.ConsumeMessage | null): any
}