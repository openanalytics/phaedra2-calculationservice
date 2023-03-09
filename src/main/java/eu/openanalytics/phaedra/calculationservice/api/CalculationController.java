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
package eu.openanalytics.phaedra.calculationservice.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import eu.openanalytics.phaedra.commons.dto.CalculationRequestDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.service.CalculationStatusService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;

@RestController
@RequiredArgsConstructor
public class CalculationController {

    private final ProtocolExecutorService protocolExecutorService;
    private final CalculationStatusService calculationStatusService;

    @PostMapping("/calculation")
    public ResponseEntity<Long> calculate(@RequestBody CalculationRequestDTO calculationRequestDTO,
                                          @RequestParam(value = "timeout", required = false) Long timeout) throws ExecutionException, InterruptedException {
        var future = protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId());
        if (timeout != null) {
            try {
                future.resultSet().get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {

            }
        }

        return new ResponseEntity<>(future.resultSetId().get(), HttpStatus.CREATED);
    }

    @GetMapping("/status")
    public ResponseEntity<CalculationStatus> status(@RequestParam(value = "resultSetId") int resultSetId) throws ResultSetUnresolvableException, ResultDataUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, PlateUnresolvableException {
        return new ResponseEntity<>(calculationStatusService.getStatus(resultSetId), HttpStatus.OK);
    }

}
