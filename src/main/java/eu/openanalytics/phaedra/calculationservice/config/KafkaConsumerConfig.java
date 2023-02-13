package eu.openanalytics.phaedra.calculationservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    public static final String PLATE_TOPIC = "plate-topic";
    public static final String PLATE_CALCULATION_EVENT = "plateCalculationEvent";
    @Bean
    public RecordFilterStrategy<String, Object> plateCalculationEventFilter() {
        RecordFilterStrategy<String, Object> recordFilterStrategy = consumerRecord -> !(consumerRecord.key().equalsIgnoreCase(PLATE_CALCULATION_EVENT));
        return recordFilterStrategy;
    }
}
