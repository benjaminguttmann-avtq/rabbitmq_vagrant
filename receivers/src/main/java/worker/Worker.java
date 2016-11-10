package workers;

import com.rabbitmq.client.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.lang.InterruptedException;

public class Worker implements Runnable {

  private static String sleeping_message = "sleep";
  private static String destroying_message ="destroy";

  boolean durableQueue = true;

    // Parse environment variables for credentials
  ConnectionFactory factory;
  Channel channel;
  Connection connection;
  int counter;
  boolean autoAck = false;
  boolean share_connection = false;
  boolean share_channel = false;
  int queuePrefatch = 1;
  String queueName = "task_queue";
  boolean perChannelPrefetch = false;
  boolean usePrefetch = true;

    // Example
    //amqp://dd875238-a26d-49a1-8ec0-f5225b7c2ecb:7ipvs3rd468dn0vo9o1g12p9at@172.28.4.30/68850f2b-266e-4c86-97a1-2d62b44c1fa4
    public Worker(Channel provided_channel, Connection provided_connection, int thread_counter, boolean autoAck, boolean share_connection, boolean share_channel, String queueName, int queuePrefatch, boolean perChannelPrefetch, boolean usePrefetch) {
      this.channel = provided_channel;
      this.connection = provided_connection;
      this.counter = thread_counter;
      this.autoAck = autoAck;
      this.share_connection = share_connection;
      this.share_channel = share_channel;
      this.queuePrefatch = queuePrefatch;
      this.queueName = queueName;
      this.perChannelPrefetch = perChannelPrefetch;
      this.usePrefetch = usePrefetch;
    }

    @Override
    public void run(){
      if (!share_connection) {
        try{
          factory = new ConnectionFactory();
          factory.setHost("localhost");
          this.connection = factory.newConnection();
        }catch(Exception e){
          System.out.println(e);
        }
      }
      if (!share_channel) {
        try{
          this.channel = connection.createChannel();
          channel.queueDeclare(queueName, durableQueue, false, false, null);
          if (usePrefetch) {
            channel.basicQos(queuePrefatch, perChannelPrefetch);
          }
        }catch(Exception e){
          System.out.println(e);
        }
      }

      System.out.println("[" + counter + "] [*] Waiting for messages. To exit press CTRL+C");

      final Consumer consumer = new DefaultConsumer(channel) {

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          String message = new String(body, "UTF-8");
          System.out.println("[" + counter + "] [x] Received '" + message + "'");
          try {
            doWork(message);
          } catch (InterruptedException e) {
            System.out.println("Close Channel");
            channel.close();
          }
            finally {
            if (!autoAck) {
              channel.basicAck(envelope.getDeliveryTag(), false);
            }
          }
        }
      };
      try {
      channel.basicConsume(queueName, autoAck, consumer);
    } catch (Exception e){
      System.out.println(e);
    }
  }
  private static void doWork(String task) throws InterruptedException {
      if (task.equals(sleeping_message)) {
        try {
          Thread.sleep(50000);
          System.out.println("sleeping");
        } catch (InterruptedException _ignored) {
          Thread.currentThread().interrupt();
        }
    } else if (task.equals(destroying_message)) {
      System.out.println("destroying");
      throw new InterruptedException();
    }
  }
}
