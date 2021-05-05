package org.b0102.opsn.scala.server

import com.rabbitmq.client.{AMQP, CancelCallback, ConnectionFactory, DeliverCallback, Delivery}
import org.b0102.opsn.protocol.Demo
import org.slf4j.LoggerFactory

import scala.util.Using

object Server
{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val RPC_QUEUE_NAME = "rpc_queue"

  private def fib(n:Int):Int =
  {
    if( n == 0 || n==1)
    {
      n
    }else fib(n -1 ) + fib( n - 2)
  }

  def main(args:Array[String]):Unit =
  {
    val factory = new ConnectionFactory();
    factory.setUri("amqp://guest:guest@localhost:5672")

    Using(factory.newConnection()){ connection =>
      Using(connection.createChannel()){ channel =>
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null)
        channel.queuePurge(RPC_QUEUE_NAME)

        channel.basicQos(1)

        logger.debug("Awaiting PRC Requests")

        val deliverCallback = new DeliverCallback()
        {
          override def handle(consumerTag: String, message: Delivery): Unit =
          {
            val rp = new AMQP.BasicProperties().builder().correlationId(message.getProperties.getCorrelationId).build()

            val content = Demo.DtoContext.parseFrom(message.getBody)
            val n = content.getN
            logger.debug("n={} {} {}", n, message.getProperties.getReplyTo, content.getName())
            val r = fib(n)
            channel.basicPublish("", message.getProperties.getReplyTo, rp, s"${r}".getBytes())
            channel.basicAck(message.getEnvelope.getDeliveryTag, false)

            factory.synchronized( {
              factory.notify()
            })

          }
        }

        val cancelCallback = new CancelCallback()
        {
          override def handle(consumerTag: String): Unit =
            {

            }
        }


        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, cancelCallback);

        while(true)
        {
          factory.synchronized( {
            factory.wait()
          })

        }



      }
    }

  }

}
