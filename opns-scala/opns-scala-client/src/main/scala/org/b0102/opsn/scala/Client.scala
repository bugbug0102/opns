package org.b0102.opsn.scala

import com.rabbitmq.client.{AMQP, CancelCallback, ConnectionFactory, DeliverCallback, Delivery}
import org.apache.commons.lang3.math.NumberUtils
import org.b0102.opsn.protocol.Demo
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import scala.util.Using


object Client
{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val RPC_QUEUE_NAME = "rpc_queue"

  class RpcClient extends AutoCloseable
  {
    private val factory = new ConnectionFactory();
    factory.setUri("amqp://guest:guest@localhost:5672")
    private val connectiono = Option(factory.newConnection())
    private val channelo = connectiono.flatMap{ c=> Option(c.createChannel())}

    def call(n:Int): Int =
    {
      val corrId = UUID.randomUUID().toString()
      channelo.map{ channel =>
        val rqn = channel.queueDeclare().getQueue
        val props = new AMQP.BasicProperties().builder().correlationId(corrId).replyTo(rqn).build()

        logger.debug("rqn {}", rqn)
        val dto = Demo.DtoContext.newBuilder()
          .setName("hello")
          .setAge(32)
          .setN(n)
          .build()

        channel.basicPublish("", RPC_QUEUE_NAME, props, dto.toByteArray)

        val response = new ArrayBlockingQueue[Int](1)

        val deliverCallback = new DeliverCallback()
        {
          override def handle(consumerTag: String, message: Delivery): Unit =
          {
            if(message.getProperties.getCorrelationId == corrId)
            {
              response.offer(NumberUtils.toInt(new String(message.getBody, "UTF-8")))
            }
          }
        }

        val cancelCallback = new CancelCallback()
        {
          override def handle(consumerTag: String): Unit =
          {

          }
        }

        val ctag = channel.basicConsume(rqn, true, deliverCallback, cancelCallback)
        val ret = response.take()
        channel.basicCancel(ctag)

        //logger.debug("ret {}", ret)
        ret

      }.getOrElse(-1)

    }

    override def close():Unit =
    {
      channelo.foreach(_.close())
      connectiono.foreach(_.close())
    }
  }

  def main(args:Array[String]):Unit =
  {
    Using(new RpcClient){ rc =>
      (0 to 10).foreach{ i =>
        val o = rc.call(i)
        logger.debug("{}->{}", i, o)
      }
    }

  }

}
