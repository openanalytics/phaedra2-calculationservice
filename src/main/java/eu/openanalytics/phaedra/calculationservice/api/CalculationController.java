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
package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.service.CalculationStatusService;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.plateservice.client.exception.UnresolvableObjectException;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class CalculationController {

    private final ProtocolExecutorService protocolExecutorService;
    private final CalculationStatusService calculationStatusService;

    @PostMapping("/calculation")
    public ResponseEntity<List<Long>> calculate(@RequestBody CalculationRequestDTO calculationRequestDTO) throws ExecutionException, InterruptedException {
        List<Long> executions = new ArrayList<>();

        for (Long plateId: calculationRequestDTO.getPlateIds()) {
            var execution = protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                plateId,
                calculationRequestDTO.getMeasIds().get(plateId));
            executions.add(execution.get());
        }
        return new ResponseEntity<>(executions, HttpStatus.CREATED);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<Long, CalculationStatus>> status(@RequestParam(value = "resultSetId") List<Long> resultSetIds) throws ResultSetUnresolvableException, ResultDataUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, UnresolvableObjectException {
        Map<Long, CalculationStatus> result = new HashMap<>();
        for (Long resultSetId: resultSetIds) {
            result.put(resultSetId, calculationStatusService.getStatus(resultSetId));
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

}
