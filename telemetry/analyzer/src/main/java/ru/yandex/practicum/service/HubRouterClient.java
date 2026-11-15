package ru.yandex.practicum.service;

import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;

@Slf4j
@Service
public class HubRouterClient {

    private final HubRouterControllerBlockingStub hubRouterClient;

    public HubRouterClient(@GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendDeviceAction(DeviceActionRequest request) {
        try {
            hubRouterClient.handleDeviceAction(request);
            log.info("Отправлена команда в HubRouter: hubId={}, scenario='{}', sensorId={}, action={}",
                    request.getHubId(),
                    request.getScenarioName(),
                    request.getAction().getSensorId(),
                    request.getAction().getType());
        } catch (StatusRuntimeException e) {
            log.error("Ошибка gRPC при отправке команды в HubRouter: {}", e.getStatus(), e);
        }
    }
}
