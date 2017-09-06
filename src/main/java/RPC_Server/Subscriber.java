package RPC_Server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.AMQBasicProperties;

import java.util.Scanner;
import java.util.UUID;

/**
 * @Author : Pushkarraj Pujari
 * @Since : 06/09/2017
 */
public class Subscriber {
    /*
    * Stage 2 - RPC Server /RPC subscriber
    * */
    public static String Queue_Name;
    public static ConnectionFactory connectionFactory;
    public static Connection connection;
    public static Channel channel;
    public static String message;
    public static String EXCHANGE_TYPE;
    public static String EXCHANGE_NAME;
    public static String BINDING_KEY;
    static {
        try{
            EXCHANGE_TYPE = "topic";
            EXCHANGE_NAME = "topic_ex";
            BINDING_KEY = "RPC";
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connection = connectionFactory.newConnection("RPC-Server");
            channel = connection.createChannel();
            message = "Appending this message to Reply Back";
        }catch (Exception exception){
            exception.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try{
            printLine("Declaring Exchange Type and Exchange Name ......");
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
            printLine("Declaring Queue ......");
            Queue_Name = channel.queueDeclare().getQueue();
            channel.queueBind(Queue_Name,EXCHANGE_NAME,BINDING_KEY);
            printLine("Waiting for Message from RPC_Client ......");
            channel.basicConsume(Queue_Name,true,new MyConsumer(channel));
            System.out.println("Press Enter to Exit");
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
