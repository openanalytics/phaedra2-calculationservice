package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.dto.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.service.protocol.ProtocolExecutorService;
import eu.openanalytics.phaedra.calculationservice.service.status.CalculationStatusService;
import eu.openanalytics.phaedra.platservice.client.exception.PlateUnresolvableException;
import eu.openanalytics.phaedra.protocolservice.client.exception.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultFeatureStatUnresolvableException;
import eu.openanalytics.phaedra.resultdataservice.client.exception.ResultSetUnresolvableException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class CalculationController {

    private final ProtocolExecutorService protocolExecutorService;
    private final CalculationStatusService calculationStatusService;

    public CalculationController(ProtocolExecutorService protocolExecutorService, CalculationStatusService calculationStatusService) {
        this.protocolExecutorService = protocolExecutorService;
        this.calculationStatusService = calculationStatusService;
    }

    @PostMapping("/calculation")
    public void calculate(@RequestBody CalculationRequestDTO calculationRequestDTO, @RequestParam(value = "timeout", required = false) Long timeout) throws ExecutionException, InterruptedException {
        var future = protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId());
        if (timeout != null) {
            try {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {

            }
        } else {
            future.get();
        }
    }

    @GetMapping("/status")
    public CalculationStatus status(@RequestParam(value = "resultSetId") int resultSetId) throws ResultSetUnresolvableException, ResultDataUnresolvableException, ResultFeatureStatUnresolvableException, ProtocolUnresolvableException, PlateUnresolvableException {
        return calculationStatusService.getStatus(resultSetId);
    }

}
