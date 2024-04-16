package com.MehdiFkih.Client;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ClientWriter {
    private final static String EXCHANGE_NAME = "write_exchange";
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                )
        {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String message;
            while(true)
            {
                System.out.println("send a message");
                message = scanner.nextLine();
                if(message.equals("exit"))
                {
                    break;
                }
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
        catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
