package stormbook

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Values, Fields}
import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client
import com.rabbitmq.client.{ConnectionFactory, QueueingConsumer}
import play.api.libs.json.Json

class RabbitMQSpout extends BaseRichSpout {

  private var connection: client.Connection = _
  private var channel: client.Channel = _
  private var consumer: QueueingConsumer = _
  private var outputCollector: SpoutOutputCollector = _
  private var objectMapper: ObjectMapper = _

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("order"))
  }

  override def open(config: util.Map[_, _],
                    context: TopologyContext,
                    outputCollector: SpoutOutputCollector): Unit = {
    this.outputCollector = outputCollector
    this.objectMapper = new ObjectMapper()
    this.connection = new ConnectionFactory().newConnection()
    this.channel = connection.createChannel()
    channel.basicQos(25)
    this.consumer = new QueueingConsumer(channel)
    channel.basicConsume("orders", false, consumer)
  }


  override def nextTuple(): Unit = {
    val delivery = consumer.nextDelivery(1L)
    if (delivery == null) return

    val order = Json.parse(delivery.getBody).as[Order]
    val msgId = delivery.getEnvelope.getDeliveryTag
    outputCollector.emit(new Values(order), msgId)
  }

  override def ack(msgId: scala.Any): Unit = {
    channel.basicAck(msgId.asInstanceOf[Long], false)
  }

  override def fail(msgId: scala.Any): Unit = {
    channel.basicReject(msgId.asInstanceOf[Long], true)
  }

  override def close(): Unit = {
    channel.close
    connection.close
  }
}
