package RPC_Client;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQBasicProperties;

import java.io.UnsupportedEncodingException;
import java.util.Scanner;
import java.util.UUID;


/**
 * @Author : Pushkarraj pujari
 * @Since : 06/09/2017
 */
public class Publisher {
    /**
     * Stage 1 - RPC Client
     * */
    public static ConnectionFactory connectionFactory;
    public static Connection connection;
    public static Channel channel;
    public static String replyQueueName;
    public static String message;
    public static AMQBasicProperties amqBasicProperties;
    public static String corrId;
    public static String EXCHANGE_TYPE;
    public static String EXCHANGE_NAME;
    public static String ROUTING_KEY;
    static {
        try{
            EXCHANGE_TYPE = "topic";
            EXCHANGE_NAME = "topic_ex";
            ROUTING_KEY = "RPC";
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connection = connectionFactory.newConnection("RPC-Client");
            channel = connection.createChannel();
            replyQueueName = channel.queueDeclare().getQueue();
            corrId = UUID.randomUUID().toString();
            message = "Hi I am RPC Client";
        }catch (Exception exception){
            exception.printStackTrace();
        }finally{
            System.out.println("Configured RPC-Publisher ......");
        }
    }
    public static void main(String[] args) {
        try{
            amqBasicProperties = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName).build();
            printLine("Declaring Exchange Type and Exchange Name ......");
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
            printLine("Publishing Message to RPC_Server ......");
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, (AMQP.BasicProperties) amqBasicProperties, message.getBytes("UTF-8"));
            printLine("Message Sent - "+message);
            printLine("Waiting for reply ...... ");
            channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException {
                    if (properties.getCorrelationId().equals(corrId)) {
                        String responseMessage = new String(body, "UTF-8");
                        System.out.println("Message Received From Server [X]" + responseMessage);
                    }
                }
            });
            System.out.println("Press Enter to Exit ...");
            new Scanner(System.in).nextLine();
            connection.close();
        }catch (Exception exception){
            exception.printStackTrace();
        }

    }

    public static void printLine(String string){
        System.out.println(string);
    }
}
