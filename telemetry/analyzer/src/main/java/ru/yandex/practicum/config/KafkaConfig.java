package ru.yandex.practicum.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaConfig {

    private String bootstrapServers;
    private Topics topics;
    private Consumers consumers;

    @Getter
    @Setter
    public static class Topics {
        private String hubs;
        private String snapshots;
    }

    @Getter
    @Setter
    public static class Consumers {
        private Map<String, String> hubs;
        private Map<String, String> snapshots;
    }

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (consumers != null && consumers.getHubs() != null) {
            consumers.getHubs().forEach((key, value) -> props.put(key.replace('-', '.'), value));
        }
        return new KafkaConsumer<>(props);
    }

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if (consumers != null && consumers.getSnapshots() != null) {
            consumers.getSnapshots().forEach((key, value) -> props.put(key.replace('-', '.'), value));
        }
        return new KafkaConsumer<>(props);
    }
}
