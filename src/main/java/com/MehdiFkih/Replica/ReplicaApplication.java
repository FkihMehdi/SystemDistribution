package com.MehdiFkih.Replica;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class ReplicaApplication {
    private final static String EXCHANGE_NAME_WRITE = "write_exchange";
    private final static String EXCHANGE_NAME_READ = "read_exchange";
    private final static String EXCHANGE_NAME_REPLICA_TO_READER = "replica_to_reader_exchange";
    private static int counter = 0;


    public static void main(String[] args) throws Exception {
        while (args.length == 0) {
            System.out.println("Please enter the FileId");
            Scanner scanner = new Scanner(System.in);
            args = new String[1];
            args[0] = scanner.nextLine();
        }
        String FileId = args[0];
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME_WRITE, "fanout");
            channel.exchangeDeclare(EXCHANGE_NAME_READ, "fanout");

            String queueNameWrite = channel.queueDeclare().getQueue();
            String queueNameRead = channel.queueDeclare().getQueue();

            channel.queueBind(queueNameWrite, EXCHANGE_NAME_WRITE, "");
            channel.queueBind(queueNameRead, EXCHANGE_NAME_READ, "");

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallbackWrite = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "' from Write Client");
                try {
                    counter++;
                    WriteInFile(counter,FileId, message);
                    System.out.println(" [x] Written '" + message + "' to File");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
            channel.basicConsume(queueNameWrite, true, deliverCallbackWrite, consumerTag -> {
            });

            DeliverCallback deliverCallbackRead = (consumerTag, delivery) -> {
                System.out.println(" [x] Read request from Read Client ");
                String message = new String(delivery.getBody(), "UTF-8");
                if (message.equals("fetch")) {
                    String lastMessage = ReadFileLastLine(FileId);
                    System.out.println(lastMessage);
                    try {
                        sendMessageToReader(lastMessage, channel);
                        System.out.println(" [x] Sent '" + message + "' to Reader Client");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            channel.basicConsume(queueNameRead, true, deliverCallbackRead, consumerTag -> {
            });

        }

    private static void WriteInFile(int counter,String FileId,String message) throws IOException {
        String filePath = "/home/mehdi/Desktop/TP3/src/main/java/com/MehdiFkih/Replica/Files/replica_" + FileId + ".txt";
        OutputStream os = null;
        try {
            os = new FileOutputStream(new File(filePath), true);
            message = counter + " " + message;
            os.write(message.getBytes(), 0, message.length());
            os.write("\n".getBytes(), 0, "\n".length());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static String ReadFileLastLine(String FileId) {
        String filePath = "/home/mehdi/Desktop/TP3/src/main/java/com/MehdiFkih/Replica/Files/replica_" + FileId + ".txt";
        String lastLine = "";
        try {
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filePath));
            String line;
            while ((line = br.readLine()) != null) {
                String [] parts = line.split(" ");
                lastLine = "";
                for(String part : parts)
                {
                    if(part.equals(parts[0]))
                    {
                        continue;
                    }
                    System.out.println("i m here");
                    lastLine += part + " ";
                }

            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastLine;
    }
    private static void sendMessageToReader(String message, Channel channel) throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME_REPLICA_TO_READER, "fanout");
        channel.basicPublish(EXCHANGE_NAME_REPLICA_TO_READER , "", null, message.getBytes("UTF-8"));
    }
}
