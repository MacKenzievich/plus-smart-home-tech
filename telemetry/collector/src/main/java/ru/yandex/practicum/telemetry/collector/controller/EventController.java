package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;
import java.util.List;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Producer<String, SpecificRecordBase> producer;

    @Value("${collector.kafka.topics.sensors:telemetry.sensors.v1}")
    private String sensorsTopic;

    @Value("${collector.kafka.topics.hubs:telemetry.hubs.v1}")
    private String hubsTopic;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            Instant timestamp = toInstant(request.getTimestamp());
            SpecificRecordBase payload = mapSensorPayload(request);

            SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                    .setId(request.getId())
                    .setHubId(request.getHubId())
                    .setTimestamp(timestamp)
                    .setPayload(payload)
                    .build();

            producer.send(new ProducerRecord<>(
                    sensorsTopic,
                    null,
                    timestamp.toEpochMilli(),
                    request.getHubId(),
                    eventAvro
            ));

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка обработки gRPC SensorEventProto", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            Instant timestamp = toInstant(request.getTimestamp());
            SpecificRecordBase payload = mapHubPayload(request);

            HubEventAvro eventAvro = HubEventAvro.newBuilder()
                    .setHubId(request.getHubId())
                    .setTimestamp(timestamp)
                    .setPayload(payload)
                    .build();

            producer.send(new ProducerRecord<>(
                    hubsTopic,
                    null,
                    timestamp.toEpochMilli(),
                    request.getHubId(),
                    eventAvro
            ));

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка обработки gRPC HubEventProto", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e)
            ));
        }
    }

    private Instant toInstant(Timestamp ts) {
        if (ts == null || (ts.getSeconds() == 0 && ts.getNanos() == 0)) {
            return Instant.now();
        }
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private SpecificRecordBase mapSensorPayload(SensorEventProto event) {
        return switch (event.getPayloadCase()) {
            case MOTION_SENSOR -> {
                MotionSensorProto p = event.getMotionSensor();
                yield MotionSensorAvro.newBuilder()
                        .setLinkQuality(p.getLinkQuality())
                        .setMotion(p.getMotion())
                        .setVoltage(p.getVoltage())
                        .build();
            }
            case TEMPERATURE_SENSOR -> {
                TemperatureSensorProto p = event.getTemperatureSensor();
                yield TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(p.getTemperatureC())
                        .setTemperatureF(p.getTemperatureF())
                        .build();
            }
            case LIGHT_SENSOR -> {
                LightSensorProto p = event.getLightSensor();
                yield LightSensorAvro.newBuilder()
                        .setLinkQuality(p.getLinkQuality())
                        .setLuminosity(p.getLuminosity())
                        .build();
            }
            case CLIMATE_SENSOR -> {
                ClimateSensorProto p = event.getClimateSensor();
                yield ClimateSensorAvro.newBuilder()
                        .setTemperatureC(p.getTemperatureC())
                        .setHumidity(p.getHumidity())
                        .setCo2Level(p.getCo2Level())
                        .build();
            }
            case SWITCH_SENSOR -> {
                SwitchSensorProto p = event.getSwitchSensor();
                yield SwitchSensorAvro.newBuilder()
                        .setState(p.getState())
                        .build();
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload события датчика");
        };
    }

    private SpecificRecordBase mapHubPayload(HubEventProto event) {
        return switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEventProto p = event.getDeviceAdded();
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(p.getId())
                        .setType(mapDeviceType(p.getType()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEventProto p = event.getDeviceRemoved();
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(p.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventProto p = event.getScenarioAdded();
                List<ScenarioConditionAvro> conditions = p.getConditionList().stream()
                        .map(this::mapCondition)
                        .toList();
                List<DeviceActionAvro> actions = p.getActionList().stream()
                        .map(this::mapAction)
                        .toList();
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(p.getName())
                        .setConditions(conditions)
                        .setActions(actions)
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEventProto p = event.getScenarioRemoved();
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(p.getName())
                        .build();
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Не задан payload события хаба");
        };
    }

    private DeviceTypeAvro mapDeviceType(DeviceTypeProto type) {
        return DeviceTypeAvro.valueOf(type.name());
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto c) {
        Object value = switch (c.getValueCase()) {
            case INT_VALUE -> c.getIntValue();
            case BOOL_VALUE -> c.getBoolValue();
            case VALUE_NOT_SET -> null;
        };
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(c.getSensorId())
                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro mapAction(DeviceActionProto a) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(a.getSensorId())
                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                .setValue(a.hasValue() ? a.getValue() : null)
                .build();
    }
}