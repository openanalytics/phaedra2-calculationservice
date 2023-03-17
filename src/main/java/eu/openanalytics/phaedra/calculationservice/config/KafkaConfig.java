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

	public static final String GROUP_ID = "calculation-service";
	
    // Topics
    public static final String TOPIC_CALCULATIONS = "calculations";
    public static final String TOPIC_PLATES = "plates";
    public static final String TOPIC_CURVEDATA = "curvedata";
    public static final String TOPIC_RESULTDATA = "resultdata";

    // Event Keys
    public static final String EVENT_REQUEST_PLATE_CALCULATION = "requestPlateCalculation";
    public static final String EVENT_REQUEST_CURVE_FIT = "requestCurveFit";
    public static final String EVENT_UPDATE_PLATE_STATUS = "requestPlateCalculationStatusUpdate";
    public static final String EVENT_SAVE_CURVE = "saveCurve";
    public static final String EVENT_SAVE_RESULT_DATA = "saveResultData";
    public static final String EVENT_SAVE_RESULT_STATS = "saveResultStats";

    @Bean
    public RecordFilterStrategy<String, Object> requestPlateCalculationFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_REQUEST_PLATE_CALCULATION));
    }
    
    @Bean
    public RecordFilterStrategy<String, Object> requestCurveFitFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_REQUEST_CURVE_FIT));
    }

    @Bean
    public RecordFilterStrategy<String, Object> saveResultDataFilter() {
        return rec -> !(rec.key().equalsIgnoreCase(EVENT_SAVE_RESULT_DATA));
    }
}
