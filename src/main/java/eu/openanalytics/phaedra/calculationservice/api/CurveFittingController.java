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

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import eu.openanalytics.curvedataservice.dto.CurveDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;

@RestController
public class CurveFittingController {

    private final CurveFittingExecutorService curveFittingExecutorService;

    public CurveFittingController(CurveFittingExecutorService curveFittingExecutorService) {
        this.curveFittingExecutorService = curveFittingExecutorService;
    }

    @PostMapping("/curvefit")
    public ResponseEntity<List<CurveDTO>> fitCurve(@RequestBody CurveFittingRequestDTO curveFittingRequestDTO) throws ExecutionException, InterruptedException {
        curveFittingExecutorService.execute(curveFittingRequestDTO);
        return ResponseEntity.ok().build();
    }
}
