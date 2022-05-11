/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
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

import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.service.TokenService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.status.CalculationStatusService;
import eu.openanalytics.phaedra.plateservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class CalculationController {

    private final ProtocolExecutorService protocolExecutorService;
    private final CalculationStatusService calculationStatusService;
    private final TokenService tokenService;

    public CalculationController(ProtocolExecutorService protocolExecutorService, CalculationStatusService calculationStatusService, TokenService tokenService) {
        this.protocolExecutorService = protocolExecutorService;
        this.calculationStatusService = calculationStatusService;
        this.tokenService = tokenService;
    }

    @PostMapping("/calculation")
    public ResponseEntity<Long> calculate(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorisationToken,
                                          @RequestBody CalculationRequestDTO calculationRequestDTO,
                                          @RequestParam(value = "timeout", required = false) Long timeout) throws ExecutionException, InterruptedException {
        var future = protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId(),
                authorisationToken);
        if (timeout != null) {
            try {
                future.resultSet().get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {

            }
        }

        return new ResponseEntity(future.resultSetId().get(), HttpStatus.CREATED);
    }

    @GetMapping("/status")
    public ResponseEntity<CalculationStatus> status(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorisationToken,
                                                    @RequestParam(value = "resultSetId") int resultSetId) throws ResultSetUnresolvableException, ResultDataUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, PlateUnresolvableException {
        return new ResponseEntity(calculationStatusService.getStatus(resultSetId, authorisationToken), HttpStatus.OK);
    }

}
