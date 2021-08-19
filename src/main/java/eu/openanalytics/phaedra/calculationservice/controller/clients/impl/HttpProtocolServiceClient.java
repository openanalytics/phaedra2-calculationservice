package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.dto.external.CalculationInputValueDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.FeatureDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ProtocolDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class HttpProtocolServiceClient implements ProtocolServiceClient {

    private static final String PHAEDRA_PROTOCOL_SERVICE = "http://PHAEDRA-PROTOCOL-SERVICE/phaedra/protocol-service";

    private final RestTemplate restTemplate;
    private final FormulaService formulaService;
    private final ModelMapper modelMapper = new ModelMapper();

    public HttpProtocolServiceClient(RestTemplate restTemplate, FormulaService formulaService) {
        this.restTemplate = restTemplate;
        this.formulaService = formulaService;
        modelMapper.typeMap(CalculationInputValueDTO.class, CalculationInputValue.class);
//        modelMapper.typeMap(Formula.class, FormulaDTO.class);
        modelMapper.validate(); // ensure that objects can be mapped
    }

    @Override
    public Optional<Protocol> getProtocol(long protocolId) {
        // 1. get protocol
        var protocol = getOrNull("/protocols/" + protocolId, ProtocolDTO.class);
        if (protocol.isEmpty()) return Optional.empty();

        // 2. get features of protocol
        var features = getListOrNull("/protocols/1/features", FeatureDTO.class);
        if (features.isEmpty()) return Optional.empty();

        // 3. get formulas corresponding to the features
        var formulas = formulaService.getFormulasByIds(
                features.get().stream()
                        .map(FeatureDTO::getFormula)
                        .collect(Collectors.toUnmodifiableList())
        );

        // 4. get CalculationInputValues corresponding to the feature
        var calculationInputValues = getListOrNull(String.format("/protocols/%s/calculationinputvalue", protocolId), CalculationInputValueDTO.class);
        if (calculationInputValues.isEmpty()) return Optional.empty(); // TODO exception here?
        var calculationInputValuesMap = calculationInputValues.get().stream()
                .map((civ) -> map(civ, new CalculationInputValue()))
                .collect(Collectors.groupingBy(
                        CalculationInputValue::getFeatureId,
                        Collectors.toList()
                ));

        // 5. create features with corresponding formulas and calculationInputValues
        var resFeatures = features.get().stream()
                .map((feature) -> {
                    var formula = formulas.get(feature.getFormula());
                    if (formula == null) {
                        throw new RuntimeException("TODO");
                    } else {
                        return new Feature(feature.getId(), feature.getName(), feature.getAlias(), feature.getDescription(),
                                feature.getFormat(), feature.getType(), feature.getSequence(), formula, calculationInputValuesMap.get(feature.getId()));
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

    @SuppressWarnings("unchecked")
    private <T> Optional<List<T>> getListOrNull(String subPath, Class<T> clazz) {
        try {
            Class<?> arrayClass = Array.newInstance(clazz, 0).getClass();
            var res = (T[]) restTemplate.getForObject(HttpProtocolServiceClient.PHAEDRA_PROTOCOL_SERVICE + subPath, arrayClass);
            if (res != null) {
                return Optional.of(Arrays.asList(res));
            }
            return Optional.empty();
        } catch (HttpClientErrorException ex) {
            System.out.println("Entity not found"); // TODO proper logging
            return Optional.empty();
        }
    }

    private CalculationInputValue map(CalculationInputValueDTO calculationInputValueDTO, CalculationInputValue calculationInputValue) {
        modelMapper.map(calculationInputValueDTO, calculationInputValue);
        return calculationInputValue;
    }

}
