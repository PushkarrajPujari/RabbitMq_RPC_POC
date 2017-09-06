
import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQBasicProperties;

import java.io.UnsupportedEncodingException;
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
    static {
        try{
            EXCHANGE_TYPE = "direct";
            EXCHANGE_NAME = "EX1";
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            connection = connectionFactory.newConnection("RPC-Client");
            channel = connection.createChannel();
            replyQueueName = channel.queueDeclare().getQueue();
            corrId = UUID.randomUUID().toString();
            message = "Hi I am RPC Client";
        }catch (Exception exception){
            exception.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try{
            amqBasicProperties = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName).build();
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
            channel.basicPublish(EXCHANGE_NAME,"", (AMQP.BasicProperties) amqBasicProperties, message.getBytes("UTF-8"));
            channel.basicConsume(replyQueueName,true,new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException {
                    if (properties.getCorrelationId().equals(corrId)) {
                        String responseMessage = new String(body, "UTF-8");
                        System.out.println("Message Received From Server [X]" + responseMessage);
                    }
                }
            });
        }catch (Exception exception){
            exception.printStackTrace();
        }

    }
}
