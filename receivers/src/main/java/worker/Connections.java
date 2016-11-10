package workers;

import com.rabbitmq.client.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.lang.InterruptedException;

public class Connections {

  //Configs
  private static final String QUEUE_NAME = "task_queue";
  private static final String uri = "amqp://tss:tss@localhost/";
  private static final boolean durableQueue = true;

  private static final int worker_count = 3;
  private static final boolean autoAck = false;
  private static final boolean share_connection = true;
  private static final boolean share_channel = false;
  private static final int queuePrefetch = 1;
  private static final boolean perChannelPrefetch = false;
  private static final boolean usePrefetch = false;

  public static void main(String[] argv) throws Exception{

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE_NAME, durableQueue, false, false, null);
    
    if (usePrefetch) {
      channel.basicQos(queuePrefetch, perChannelPrefetch);
    }

    ExecutorService executor = Executors.newFixedThreadPool(worker_count);

    for (int i = 1; i <= worker_count; i++ ) {
      System.out.println("Starting worker " + i);
      Runnable worker = new Worker(channel, connection, i, autoAck, share_connection, share_channel, QUEUE_NAME, queuePrefetch, perChannelPrefetch, usePrefetch);
      executor.execute(worker);
    }
    executor.shutdown();
    while(!executor.isTerminated()){
    }
    System.out.println("Finished Thread");
  }
}
