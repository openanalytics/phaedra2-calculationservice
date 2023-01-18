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

import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.status.CalculationStatusService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class CurveFittingController {

    private final CurveFittingExecutorService curveFittingExecutorService;
    private final CalculationStatusService calculationStatusService;


    public CurveFittingController(CurveFittingExecutorService curveFittingExecutorService, CalculationStatusService calculationStatusService) {
        this.curveFittingExecutorService = curveFittingExecutorService;
        this.calculationStatusService = calculationStatusService;
    }
    @PostMapping("/curvefit")
    public ResponseEntity<List<CurveDTO>> fitCurve(@RequestBody CurveFittingRequestDTO curveFittingRequestDTO,
                                                   @RequestParam(value = "timeout", required = false) Long timeout) throws ExecutionException, InterruptedException {
        var future = curveFittingExecutorService.execute(
                curveFittingRequestDTO.getProtocolId(),
                curveFittingRequestDTO.getPlateId(),
                curveFittingRequestDTO.getResultSetId(),
                curveFittingRequestDTO.getMeasId());
        if (timeout != null) {
            try {
                future.plateCurves().get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {

            }
        }

        return new ResponseEntity<>(future.plateCurves().get(), HttpStatus.CREATED);
    }
}
