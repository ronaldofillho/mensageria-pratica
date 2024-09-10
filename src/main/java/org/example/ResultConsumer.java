package org.example;

import com.rabbitmq.client.*;

public class ResultConsumer {

    private final static String RESULT_QUEUE_NAME = "result_queue";
    private static long firstTimestamp = 0;
    private static long lastTimestamp = 0;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(RESULT_QUEUE_NAME, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                String[] parts = message.split("-");
                long sentTimestamp = Long.parseLong(parts[1]);
                int messageNumber = Integer.parseInt(parts[0]);

                if (messageNumber == 1) {
                    firstTimestamp = sentTimestamp;
                    System.out.println("Received first message with timestamp: " + firstTimestamp);
                } else if (messageNumber == 1000000) {
                    lastTimestamp = sentTimestamp;
                    System.out.println("Received last message with timestamp: " + lastTimestamp);

                    // Calcular a diferença de tempo entre a primeira e a última mensagem
                    long totalTime = lastTimestamp - firstTimestamp;
                    System.out.println("Total time between first and last message: " + totalTime + " ms");
                }

                // Acknowledge após o processamento
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (Exception e) {
                System.err.println("Failed to process message: " + e.getMessage());
            }
        };

        channel.basicConsume(RESULT_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }
}