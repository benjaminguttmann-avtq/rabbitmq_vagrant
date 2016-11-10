package newTask;

import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;

import java.util.Map;

public class NewTask {

  private static final String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] argv) throws Exception {
    boolean durableQueue = true;

    int message_count = 10000;
    final String uri = "amqp://admin:admin@localhost:5555/";

      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
      Connection connection = factory.newConnection();
      final Channel channel = connection.createChannel();
      int counter = 0;
     channel.queueDeclare(TASK_QUEUE_NAME, durableQueue, false, false, null);
      for (int i = 0;i < message_count; i++){
      String[] arguments = {"Hello World", Integer.toString(counter)};
      String message = getMessage(arguments);

      channel.basicPublish("", TASK_QUEUE_NAME,
          MessageProperties.PERSISTENT_TEXT_PLAIN,
          message.getBytes("UTF-8"));
      System.out.println(" [x] Sent '" + message + "'");
      counter++;
    }
      channel.close();
      connection.close();
  }
  private static String getMessage(String[] strings) {
  if (strings.length < 1)
    return "Hello World!";
  return joinStrings(strings, " ");
}

private static String joinStrings(String[] strings, String delimiter) {
  int length = strings.length;
  if (length == 0) return "";
  StringBuilder words = new StringBuilder(strings[0]);
  for (int i = 1; i < length; i++) {
    words.append(delimiter).append(strings[i]);
  }
  return words.toString();
}
}
