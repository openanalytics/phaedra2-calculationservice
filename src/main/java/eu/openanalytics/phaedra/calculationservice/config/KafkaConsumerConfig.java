package eu.openanalytics.phaedra.calculationservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    public static final String CALCULATIONS_TOPIC = "calculations";
    public static final String PLATE_TOPIC = "plate-topic";
    public static final String CURVEDATA_TOPIC = "curvedata-topic";
    public static final String PLATE_CALCULATION_EVENT = "plateCalculationEvent";
    public static final String UPDATE_PLATE_STATUS_EVENT = "updatePlateCalculationStatus";
    public static final String CURVE_FIT_EVENT = "curveFitEvent";
    public static final String SAVE_CURVE_EVENT = "createCurve";

    @Bean
    public RecordFilterStrategy<String, Object> plateCalculationEventFilter() {
        RecordFilterStrategy<String, Object> recordFilterStrategy = consumerRecord -> !(consumerRecord.key().equalsIgnoreCase(PLATE_CALCULATION_EVENT));
        return recordFilterStrategy;
    }
    @Bean
    public RecordFilterStrategy<String, Object> curveFitEventFilter() {
        RecordFilterStrategy<String, Object> recordFilterStrategy = consumerRecord -> !(consumerRecord.key().equalsIgnoreCase(CURVE_FIT_EVENT));
        return recordFilterStrategy;
    }
}
