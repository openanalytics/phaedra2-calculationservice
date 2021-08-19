package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.dto.external.FeatureDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ProtocolDTO;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class HttpProtocolServiceClient implements ProtocolServiceClient {

    private static final String PHAEDRA_PROTOCOL_SERVICE = "http://PHAEDRA-PROTOCOL-SERVICE/phaedra/protocol-service";

    private final RestTemplate restTemplate;
    private final FormulaService formulaService;

    public HttpProtocolServiceClient(RestTemplate restTemplate, FormulaService formulaService) {
        this.restTemplate = restTemplate;
        this.formulaService = formulaService;
    }

    @Override
    public Optional<Protocol> getProtocol(long protocolId) {
        // 1. get protocol
        var protocol = getOrNull("/protocols/" + protocolId, ProtocolDTO.class);
        if (protocol.isEmpty()) return Optional.empty();

        // 2. get features of protocol
        var features = getOrNull("/protocols/1/features", FeatureDTO[].class);
        if (features.isEmpty()) return Optional.empty();

        // 3. get formulas corresponding to the features
        var formulas = formulaService.getFormulasByIds(
                Arrays.stream(features.get())
                        .map(FeatureDTO::getFormula)
                        .collect(Collectors.toUnmodifiableList())
        );

        // 4. create features with corresponding formulas
        var resFeatures = Arrays.stream(features.get()).map((feature) -> {
            var formula = formulas.get(feature.getFormula());
            if (formula == null) {
                throw new RuntimeException("TODO");
            } else {
                return new Feature(feature.getId(), feature.getName(), feature.getAlias(), feature.getDescription(),
                        feature.getFormat(), feature.getType(), feature.getSequence(), formula);
            }
        }).collect(Collectors.toUnmodifiableList());

        // 5. create protocol
        return protocol.map((p) -> new Protocol(
                p.getId(),
                p.getName(),
                p.getDescription(),
                p.isEditable(),
                p.isInDevelopment(),
                p.getLowWelltype(),
                p.getHighWelltype(),
                resFeatures));
    }

    private <T> Optional<T> getOrNull(String subPath, Class<T> clazz) {
        try {
            return Optional.ofNullable(restTemplate.getForObject(HttpProtocolServiceClient.PHAEDRA_PROTOCOL_SERVICE + subPath, clazz));
        } catch (HttpClientErrorException ex) {
            System.out.println("Entity not found"); // TODO proper logging
            return Optional.empty();
        }
    }

}
