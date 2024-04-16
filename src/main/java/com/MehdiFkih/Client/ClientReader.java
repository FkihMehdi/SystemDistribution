package com.MehdiFkih.Client;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ClientReader {
    private final static String EXCHANGE_NAME = "read_exchange";
    private final static String EXCHANGE_NAME_REPLICA_TO_READER = "replica_to_reader_exchange";

    private static boolean received = false ;


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

                if (message.equals("Read Last")) {

                    channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "' request");

                    System.out.println(" [*] Waiting for message.");

                    DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                        String messageReceived = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        if(!received )
                            System.out.println(" [x] Received '" + messageReceived + "' from Write Client");
                        received = true;

                    };
                    channel.basicConsume(queueNameRead, true, deliverCallbackWrite, consumerTag -> {
                    });
                    received = false;

                }

            }
        }
    }
}