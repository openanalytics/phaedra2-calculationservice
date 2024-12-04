/**
 * Phaedra II
 *
 * Copyright (C) 2016-2024 Open Analytics
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
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;

@Configuration
@EnableKafka
public class KafkaConfig {

	public static final String GROUP_ID = "calculation-service";

    // Topics
    public static final String TOPIC_CALCULATIONS = "calculations";
    public static final String TOPIC_PLATES = "plates";
    public static final String TOPIC_CURVEDATA = "curvedata";
    public static final String TOPIC_RESULTDATA = "resultdata";
    public static final String TOPIC_SCRIPTENGINE_REQUESTS = "scriptengine-requests";
    public static final String TOPIC_SCRIPTENGINE_UPDATES = "scriptengine-updates";

    // Event Keys
    public static final String EVENT_REQUEST_PLATE_CALCULATION = "requestPlateCalculation";
    public static final String EVENT_REQUEST_CURVE_FIT = "requestCurveFit";
    public static final String EVENT_UPDATE_PLATE_STATUS = "requestPlateCalculationStatusUpdate";
    public static final String EVENT_SAVE_CURVE = "saveCurve";

    public static final String EVENT_NOTIFY_CALCULATION_EVENT = "notifyCalculationEvent";

    public static final String EVENT_SAVE_RESULT_DATA = "saveResultData";
    public static final String EVENT_SAVE_RESULT_STATS = "saveResultStats";
    public static final String EVENT_RESULT_SET_UPDATED = "resultSetUpdated";
    public static final String EVENT_RESULT_DATA_UPDATED = "resultDataUpdated";
    public static final String EVENT_RESULT_FEATURE_STAT_UPDATED = "resultFeatureStatUpdated";

    @Bean
    public RecordFilterStrategy<String, Object> requestPlateCalculationFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_REQUEST_PLATE_CALCULATION));
    }

    @Bean
    public RecordFilterStrategy<String, Object> requestCurveFitFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_REQUEST_CURVE_FIT));
    }

    @Bean
    public RecordFilterStrategy<String, Object> resultSetUpdatedFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_RESULT_SET_UPDATED));
    }

    @Bean
    public RecordFilterStrategy<String, Object> resultDataUpdatedFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_RESULT_DATA_UPDATED));
    }

    @Bean
    public RecordFilterStrategy<String, Object> resultFeatureStatUpdatedFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_RESULT_FEATURE_STAT_UPDATED));
    }

    @Bean
    public BytesJsonMessageConverter messageConverter() {
    	return new BytesJsonMessageConverter();
    }
}
