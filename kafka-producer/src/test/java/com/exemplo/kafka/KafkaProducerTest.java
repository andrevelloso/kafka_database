package com.exemplo.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

public class KafkaProducerTest {

    private static final Faker faker = new Faker();

    @Test
    public void testProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.35.180:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        MockKafkaServer.sendMessages(mockProducer, 10, 0.5);

        // Verifica se as mensagens foram enviadas
        assertEquals(10, mockProducer.history().size());
        
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = mockProducer.history().get(i);
            assertEquals("key" + i, record.key());
            String[] parts = record.value().split(",");
            assertEquals(3, parts.length);
            assertNotNull(parts[0]);
            assertNotNull(parts[1]);
            assertNotNull(parts[2]);
            assertTrue(parts[0].length() > 0); // Verifica se o primeiro nome não está vazio
            assertTrue(parts[1].length() > 0); // Verifica se a cidade não está vazia
            assertTrue(parts[2].length() > 0); // Verifica se o nome da empresa não está vazio
        }
    }
}

class MockKafkaServer {
    private static final Faker faker = new Faker();

    public static void sendMessages(MockProducer<String, String> producer, int messageCount, double delaySeconds) {
        for (int i = 0; i < messageCount; i++) {
            String key = "key" + i;
            String value = generateRandomMessage();
            ProducerRecord<String, String> record = new ProducerRecord<>("users", key, value);
            producer.send(record);

            try {
                Thread.sleep((long) (delaySeconds * 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
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