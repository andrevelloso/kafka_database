package com.exemplo.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MockKafkaServer {

    private static final Logger logger = LoggerFactory.getLogger(MockKafkaServer.class);
    private static final Faker faker = new Faker();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.35.180:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String topic = "users"; // Nome do tópico

        int messageCount = 100;
        double delaySeconds = 0.5;

        if (args.length > 0) {
            try {
                messageCount = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                logger.warn("Argumento de contagem de mensagens inválido, usando padrão: 100");
            }
        }

        if (args.length > 1) {
            try {
                delaySeconds = Double.parseDouble(args[1]);
            } catch (NumberFormatException e) {
                logger.warn("Argumento de atraso inválido, usando padrão: 0,5 segundos");
            }
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String key = "key" + i;
                String value = generateRandomMessage();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Mensagem enviada com sucesso: topic={}, key={}, value={}, offset={}",
                                topic, key, value, metadata.offset());
                    } else {
                        logger.error("Erro ao enviar mensagem: topic={}, key={}, value={}", topic, key, value, exception);
                    }
                });

                try {
                    TimeUnit.MILLISECONDS.sleep((long) (delaySeconds * 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrompida", e);
                }
            }
        }
    }

    private static String generateRandomMessage() {
        String param1 = sanitize(faker.name().firstName());
        String param2 = sanitize(faker.address().city());
        String param3 = sanitize(faker.company().name());
        return String.format("%s,%s,%s", param1, param2, param3);
    }

    private static String sanitize(String input) {
        return input.replace(",", "");
    }
}