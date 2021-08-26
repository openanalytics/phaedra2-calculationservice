package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.CalculationRequestDTO;
import eu.openanalytics.phaedra.calculationservice.service.ProtocolExecutorService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class CalculationController {

    private final ProtocolExecutorService protocolExecutorService;

    public CalculationController(ProtocolExecutorService protocolExecutorService) {
        this.protocolExecutorService = protocolExecutorService;
    }

    @PostMapping("/calculation")
    public void calculate(@RequestBody CalculationRequestDTO calculationRequestDTO) throws ExecutionException, InterruptedException {
        protocolExecutorService.execute(
                calculationRequestDTO.getProtocolId(),
                calculationRequestDTO.getPlateId(),
                calculationRequestDTO.getMeasId()).get();
    }

}
