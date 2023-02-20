package eu.openanalytics.phaedra.calculationservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

@Configuration
@EnableKafka
public class KafkaConfig {
    // Kafka topics
    public static final String CALCULATIONS_TOPIC = "calculations";
    public static final String PLATE_TOPIC = "plate-topic";
    public static final String CURVEDATA_TOPIC = "curvedata-topic";
    public static final String RESULTDATA_TOPIC = "resultdata-topic";

    // Kafka events
    public static final String PLATE_CALCULATION_EVENT = "plateCalculationEvent";
    public static final String UPDATE_PLATE_STATUS_EVENT = "updatePlateCalculationStatus";
    public static final String CURVE_FIT_EVENT = "curveFitEvent";
    public static final String SAVE_CURVE_EVENT = "createCurve";
    public static final String SAVE_FEATURE_RESULTDATA_EVENT = "saveResultDataEvent";
    public static final String SAVE_FEATURE_STATS_EVENT = "saveResultStatsEvent";


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

    @Bean
    public RecordFilterStrategy<String, Object> saveResultDataEventFilter() {
        RecordFilterStrategy<String, Object> recordFilterStrategy = consumerRecord -> !(consumerRecord.key().equalsIgnoreCase(SAVE_FEATURE_RESULTDATA_EVENT));
        return recordFilterStrategy;
    }
}
