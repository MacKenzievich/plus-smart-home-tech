package ru.yandex.practicum.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final Map<String, HubEventHandler> handlers;

    @Value("${analyzer.kafka.topics.hubs}")
    private String hubsTopic;

    public HubEventProcessor(KafkaConsumer<String, HubEventAvro> consumer, List<HubEventHandler> handlerList) {
        this.consumer = consumer;
        this.handlers = handlerList.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(hubsTopic));
            log.info("HubEventProcessor подписался на топик: {}", hubsTopic);

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    if (event == null || event.getPayload() == null) {
                        continue;
                    }

                    String payloadType = event.getPayload().getClass().getName();
                    HubEventHandler handler = handlers.get(payloadType);

                    if (handler != null) {
                        handler.handle(event);
                    } else {
                        log.warn("Не найден обработчик для события хаба типа: {}", payloadType);
                    }
                }
            }
        } catch (WakeupException ignored) {
            log.info("Получен сигнал остановки для HubEventProcessor");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            log.info("Закрываем консьюмер событий хаба");
            consumer.close();
        }
    }
}
