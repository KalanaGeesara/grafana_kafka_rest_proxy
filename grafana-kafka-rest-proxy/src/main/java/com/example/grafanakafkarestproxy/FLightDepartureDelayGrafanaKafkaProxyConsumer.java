package com.accelaero.ops.manager.integration.consumer;

import com.accelaero.ops.manager.data.FlightRepository;
import com.accelaero.ops.manager.data.GrafanaAlertRepository;
import com.accelaero.ops.manager.integration.adapter.GrafanaMetricProcessor;
import com.accelaero.ops.manager.model.GrafanaAlert;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

@Component
@ConditionalOnExpression(
        "!T(org.springframework.util.StringUtils).isEmpty('${spring.kafka.bootstrap-servers}')")
@RequiredArgsConstructor
@Slf4j
public class FLightDepartureDelayGrafanaKafkaProxyConsumer {

    private final GrafanaMetricProcessor grafanaMetricProcessor;

    private final GrafanaAlertRepository grafanaAlertRepository;

    private final FlightRepository flightRepository;

    @KafkaListener(topics = "${aero.ops.flight-departure-delay-grafana-topic}",
            containerFactory = "kafkaConnectListenerContainerFactory")
    public void processFuelPriceMessage(Object alert) {
        log.info("Fuel Price Kafka Message: {}" ,alert);
        ConsumerRecord consumerRecord = (ConsumerRecord) alert;
        if(consumerRecord.value() == null){
            return;
        }

        try {
            String consumedMessage = (String) consumerRecord.value();
            List<String> generatedMessages = grafanaMetricProcessor.getGeneratedMessages(consumedMessage);

            generatedMessages.stream().forEach(generatedMessage -> {
                try {
                    grafanaAlertRepository.saveAndFlush(getGrafanaAlert(generatedMessage));
                } catch (JSONException e) {
                    log.error("unable to adapt grafana alert message : ", e);
                }
            });

        } catch (JSONException e) {
            log.error("unable to adapt grafana alert message : ", e);
        } catch (Exception ex ) {
            log.error("Error in grafana alert consumer : ", ex);
        }
    }

    private GrafanaAlert getGrafanaAlert(String consumedMessage) throws JSONException {
        GrafanaAlert grafanaAlert = new GrafanaAlert();
        grafanaAlert.setCreatedAt(new Timestamp(System.currentTimeMillis()));
        grafanaAlert.setStatus(grafanaMetricProcessor.getValue(consumedMessage, "Status"));
        grafanaAlert.setFlight(flightRepository.readByFlightReference(grafanaMetricProcessor.getValue(consumedMessage,"flightReference")).get());
        grafanaAlert.setAction("Pending");
        return grafanaAlert;
    }
}
