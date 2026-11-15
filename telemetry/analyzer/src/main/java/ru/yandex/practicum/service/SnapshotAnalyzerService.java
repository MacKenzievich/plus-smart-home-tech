package ru.yandex.practicum.service;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.ConditionOperation;
import ru.yandex.practicum.model.ConditionType;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotAnalyzerService {

    private static final Map<ConditionType, Function<Object, Optional<Integer>>> VALUE_EXTRACTORS = Map.of(
            ConditionType.MOTION, data -> (data instanceof MotionSensorAvro m) ? Optional.of(m.getMotion() ? 1 : 0) : Optional.empty(),
            ConditionType.LUMINOSITY, data -> (data instanceof LightSensorAvro l) ? Optional.of(l.getLuminosity()) : Optional.empty(),
            ConditionType.SWITCH, data -> (data instanceof SwitchSensorAvro s) ? Optional.of(s.getState() ? 1 : 0) : Optional.empty(),
            ConditionType.TEMPERATURE, data -> {
                if (data instanceof TemperatureSensorAvro t) {
                    return Optional.of(t.getTemperatureC());
                }
                if (data instanceof ClimateSensorAvro c) {
                    return Optional.of(c.getTemperatureC());
                }
                return Optional.empty();
            },
            ConditionType.CO2LEVEL, data -> (data instanceof ClimateSensorAvro c) ? Optional.of(c.getCo2Level()) : Optional.empty(),
            ConditionType.HUMIDITY, data -> (data instanceof ClimateSensorAvro c) ? Optional.of(c.getHumidity()) : Optional.empty()
    );

    private static final Map<ConditionOperation, BiPredicate<Integer, Integer>> OPERATIONS = Map.of(
            ConditionOperation.EQUALS, Integer::equals,
            ConditionOperation.GREATER_THAN, (actual, expected) -> actual > expected,
            ConditionOperation.LOWER_THAN, (actual, expected) -> actual < expected
    );

    private final ScenarioRepository scenarioRepository;
    private final HubRouterClient hubRouterClient;

    public void processSnapshot(SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> states = snapshot.getSensorsState();
        if (states == null || states.isEmpty()) {
            return;
        }

        List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());
        if (scenarios.isEmpty()) {
            return;
        }

        Timestamp now = Timestamp.newBuilder()
                .setSeconds(Instant.now().getEpochSecond())
                .setNanos(Instant.now().getNano())
                .build();

        scenarios.stream()
                .filter(scenario -> isTriggered(scenario, states))
                .flatMap(scenario -> buildActionRequests(scenario, now))
                .forEach(hubRouterClient::sendDeviceAction);
    }

    private boolean isTriggered(Scenario scenario, Map<String, SensorStateAvro> states) {
        Map<Sensor, Condition> conditions = scenario.getConditions();
        return conditions != null
                && !conditions.isEmpty()
                && conditions.entrySet().stream()
                .allMatch(entry -> checkCondition(entry.getKey(), entry.getValue(), states));
    }

    private boolean checkCondition(Sensor sensor, Condition condition, Map<String, SensorStateAvro> states) {
        if (condition.getValue() == null) {
            return false;
        }

        return Optional.ofNullable(states.get(sensor.getId()))
                .map(SensorStateAvro::getData)
                .flatMap(data -> VALUE_EXTRACTORS.getOrDefault(condition.getType(), d -> Optional.empty()).apply(data))
                .map(actual -> OPERATIONS.get(condition.getOperation()).test(actual, condition.getValue()))
                .orElse(false);
    }

    private Stream<DeviceActionRequest> buildActionRequests(Scenario scenario, Timestamp timestamp) {
        return scenario.getActions().entrySet().stream()
                .map(entry -> {
                    Sensor sensor = entry.getKey();
                    Action action = entry.getValue();

                    DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                            .setSensorId(sensor.getId())
                            .setType(ActionTypeProto.valueOf(action.getType().name()));

                    if (action.getValue() != null) {
                        actionBuilder.setValue(action.getValue());
                    }

                    return DeviceActionRequest.newBuilder()
                            .setHubId(scenario.getHubId())
                            .setScenarioName(scenario.getName())
                            .setAction(actionBuilder.build())
                            .setTimestamp(timestamp)
                            .build();
                });
    }
}
