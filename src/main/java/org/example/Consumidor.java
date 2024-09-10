package org.example;

import com.rabbitmq.client.*;

public class Consumidor {

    private final static String QUEUE_NAME = "non_durable_queue";
    private final static String RESULT_QUEUE_NAME = "result_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(RESULT_QUEUE_NAME, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                String[] parts = message.split("-");
                long sentTimestamp = Long.parseLong(parts[1]);
                long receivedTimestamp = System.currentTimeMillis();
                int messageNumber = Integer.parseInt(parts[0]);

                // Imprimir a latência para todas as mensagens
                long latency = receivedTimestamp - sentTimestamp;
                System.out.println("Received message " + messageNumber + " with latency: " + latency + " ms");

                // Colocar as mensagens 1 e 1 milhão em uma nova fila para processamento posterior
                if (messageNumber == 1 || messageNumber == 1000000) {
                    channel.basicPublish("", RESULT_QUEUE_NAME, null, message.getBytes());
                    System.out.println("Message " + messageNumber + " forwarded to result queue.");
                }

                // Acknowledge após o processamento
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (Exception e) {
                System.err.println("Failed to process message: " + e.getMessage());
                // Se falhar, pode-se usar basicNack para rejeitar a mensagem sem reencaminhamento
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }
}