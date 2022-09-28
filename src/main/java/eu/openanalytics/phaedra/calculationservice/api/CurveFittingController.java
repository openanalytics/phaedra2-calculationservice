package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.CurveFittingRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.protocol.CurveFittingExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.status.CalculationStatusService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    public ResponseEntity<Long> fitCurve(@RequestBody CurveFittingRequestDTO curveFittingRequestDTO,
                                         @RequestParam(value = "timeout", required = false) Long timeout) throws ExecutionException, InterruptedException {
        var future = curveFittingExecutorService.execute(
                curveFittingRequestDTO.getProtocolId(),
                curveFittingRequestDTO.getPlateId(),
                curveFittingRequestDTO.getResultSetId(),
                curveFittingRequestDTO.getMeasId());
        if (timeout != null) {
            try {
                future.curve().get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {

            }
        }

        return new ResponseEntity<>(future.curveId().get(), HttpStatus.CREATED);
    }
}
