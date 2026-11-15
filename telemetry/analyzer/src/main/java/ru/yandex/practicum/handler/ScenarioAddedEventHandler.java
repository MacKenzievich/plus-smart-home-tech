package ru.yandex.practicum.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.ActionType;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.ConditionOperation;
import ru.yandex.practicum.model.ConditionType;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.model.ScenarioConditionId;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;

    @Override
    public String getMessageType() {
        return ScenarioAddedEventAvro.class.getName();
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        String hubId = event.getHubId();
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();
        String scenarioName = payload.getName();

        Set<String> sensorIds = new HashSet<>();
        payload.getConditions().forEach(c -> sensorIds.add(c.getSensorId()));
        payload.getActions().forEach(a -> sensorIds.add(a.getSensorId()));

        if (!sensorRepository.existsByIdInAndHubId(sensorIds, hubId)) {
            log.warn("Не все датчики для сценария '{}' найдены у хаба {}", scenarioName, hubId);
            return;
        }

        Map<String, Sensor> sensorsMap = sensorRepository.findAllById(sensorIds).stream()
                .collect(Collectors.toMap(Sensor::getId, Function.identity()));

        scenarioRepository.findByHubIdAndName(hubId, scenarioName)
                .ifPresent(scenario -> {
                    scenarioRepository.delete(scenario);
                    scenarioRepository.flush();
                });

        Map<Sensor, Action> actionsMap = new HashMap<>();
        for (DeviceActionAvro actAvro : payload.getActions()) {
            Action action = Action.builder()
                    .type(ActionType.valueOf(actAvro.getType().name()))
                    .value(actAvro.getValue())
                    .build();
            actionsMap.put(sensorsMap.get(actAvro.getSensorId()), action);
        }

        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(scenarioName)
                .actions(actionsMap)
                .build();

        List<ScenarioCondition> scenarioConditions = new ArrayList<>();
        for (ScenarioConditionAvro condAvro : payload.getConditions()) {
            Condition condition = Condition.builder()
                    .type(ConditionType.valueOf(condAvro.getType().name()))
                    .operation(ConditionOperation.valueOf(condAvro.getOperation().name()))
                    .value(mapConditionValue(condAvro.getValue()))
                    .build();
            Sensor sensor = sensorsMap.get(condAvro.getSensorId());
            scenarioConditions.add(ScenarioCondition.builder()
                    .id(new ScenarioConditionId())
                    .scenario(scenario)
                    .sensor(sensor)
                    .condition(condition)
                    .build());
        }
        scenario.setConditions(scenarioConditions);

        scenarioRepository.save(scenario);
        log.info("Сохранён сценарий '{}' для хаба {}", scenarioName, hubId);
    }

    private Integer mapConditionValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Boolean b) {
            return b ? 1 : 0;
        }
        return null;
    }
}
