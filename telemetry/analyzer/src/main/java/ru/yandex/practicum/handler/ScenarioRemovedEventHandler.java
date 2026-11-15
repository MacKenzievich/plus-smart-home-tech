package ru.yandex.practicum.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.repository.ScenarioRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;

    @Override
    public String getMessageType() {
        return ScenarioRemovedEventAvro.class.getName();
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioRemovedEventAvro payload = (ScenarioRemovedEventAvro) event.getPayload();
        String hubId = event.getHubId();
        String scenarioName = payload.getName();

        scenarioRepository.findByHubIdAndName(hubId, scenarioName).ifPresent(scenario -> {
            scenarioRepository.delete(scenario);
            log.info("Удалён сценарий '{}' для хаба {}", scenarioName, hubId);
        });
    }
}
