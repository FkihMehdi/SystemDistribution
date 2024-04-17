package com.MehdiFkih.Client;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;


public class ClientReader {
    private final static String EXCHANGE_NAME = "read_exchange";
    private final static String EXCHANGE_NAME_REPLICA_TO_READER = "replica_to_reader_exchange";
    private static Map<String,Integer> map = new HashMap<>();


    public static void main(String[] argv) throws Exception {
        Scanner scanner = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.exchangeDeclare(EXCHANGE_NAME_REPLICA_TO_READER, "fanout");

            String queueNameRead = channel.queueDeclare().getQueue();
            channel.queueBind(queueNameRead, EXCHANGE_NAME_REPLICA_TO_READER, "");

            String message = "";
            System.out.print("To exit, type 'exit'\n");
            System.out.print("Pour envoyer une requete, tapez 'Read Last'\n");

            while (true) {
                System.out.print("Enter un choix: \n");
                message = scanner.nextLine();
                if (message.equals("exit")) {
                    break;
                }

                if (message.equals("fetch")) {
                    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "' request");

                    System.out.println(" [*] Waiting for message.");

                    DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                        String messageReceived = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        //System.out.println(" [x] Received '" + messageReceived + "' from Replica");
                        map.put(messageReceived, map.getOrDefault(messageReceived, 0) + 1);
                    };
                    channel.basicConsume(queueNameRead, true, deliverCallbackWrite, consumerTag -> {
                    });
                }else if (message.equals("Read Last")) {
                    printLast(map);
                } else {
                    System.out.println("Invalid input");
                }

            }
        }
    }
    private static void printLast(Map<String,Integer> map){
        if(map.isEmpty()){
            System.out.println("No message received");
        }else{
            String lastMessage = "";
            for(Map.Entry<String,Integer> entry : map.entrySet()){
                System.out.println("message : "+entry.getKey()+" count : "+entry.getValue());
                if(entry.getValue()>=2)
                {
                    lastMessage = entry.getKey();
                }
            }
            if(lastMessage.equals(""))
            {
                System.out.println("No message received more than once ,  so maybe two replicas are not running");
            }else {
                System.out.println("Last message received more than once is : " + lastMessage);
            }
            map.clear();
        }
    }
}