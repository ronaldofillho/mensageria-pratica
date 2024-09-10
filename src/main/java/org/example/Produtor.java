package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP;

public class Produtor {

    private final static String QUEUE_NAME = "non_durable_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            // Define mensagens não persistentes explicitamente
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(1) // Não persistente
                    .build();

            for (int i = 1; i <= 1000000; i++) {
                String message = i + "-" + System.currentTimeMillis();
                channel.basicPublish("", QUEUE_NAME, properties, message.getBytes());

                // Exibe progresso a cada 100k mensagens
                if (i % 100000 == 0) {
                    System.out.println(i + " mensagens enviadas.");
                }
            }
        }
    }
}
