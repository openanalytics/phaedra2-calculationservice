/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
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
