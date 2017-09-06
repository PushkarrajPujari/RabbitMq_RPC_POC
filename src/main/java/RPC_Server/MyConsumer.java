package RPC_Server;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @Author : Pushkarraj Pujari
 * @Since : 06/09/2017
 */
public class MyConsumer extends DefaultConsumer {
    private String message;
    private String response;
    private Channel channel;
    private AMQP.BasicProperties basicProperties;

    public MyConsumer(Channel channel) {
        super(channel);
        this.channel = channel;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        try{
            basicProperties = new AMQP.BasicProperties.Builder().correlationId(properties.getCorrelationId()).build();
            message = new String(body);
            response = message + " || I got your message , thanks for writing to the server ";
            show();
        }catch (Exception exception){
            exception.printStackTrace();
        }finally {
            Subscriber.printLine("Replying back to the client");
            channel.basicPublish( "", properties.getReplyTo(), basicProperties, response.getBytes("UTF-8"));
        }
    }

    public void show(){
        System.out.println("Message Received @ RPC_Server [X] = "+message);
        System.out.println("RPC_Server will reply back ....... ");
    }

}
