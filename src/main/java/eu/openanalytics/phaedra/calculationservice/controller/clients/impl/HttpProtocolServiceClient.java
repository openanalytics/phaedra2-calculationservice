package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.CalculationInputValueDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.FeatureDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ProtocolDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class HttpProtocolServiceClient implements ProtocolServiceClient {

    private final RestTemplate restTemplate;
    private final FormulaService formulaService;
    private final ModelMapper modelMapper = new ModelMapper();

    public HttpProtocolServiceClient(RestTemplate restTemplate, FormulaService formulaService) {
        this.restTemplate = restTemplate;
        this.formulaService = formulaService;
        modelMapper.typeMap(CalculationInputValueDTO.class, CalculationInputValue.class);
        modelMapper.validate(); // ensure that objects can be mapped
    }

    @Override
    public Protocol getProtocol(long protocolId) throws ProtocolUnresolvableException {
        // 1. get protocol
        var protocol = get(protocolId);

        // 2. get features
        var features = getFeatures(protocolId);

        // 3. get formulas corresponding to the features
        var formulaIds = features.stream()
                .map(FeatureDTO::getFormulaId)
                .collect(Collectors.toUnmodifiableList());
        var formulas = formulaService.getFormulasByIds(formulaIds);

        // 4. get CalculationInputValues corresponding to the feature
        var calculationInputValues = getCivs(protocolId);
        // -> convert it to a map for easier lookup
        var calculationInputValuesMap = calculationInputValues
                .stream()
                .map((civ) -> map(civ, new CalculationInputValue()))
                .collect(Collectors.groupingBy(
                        CalculationInputValue::getFeatureId,
                        Collectors.toList()
                ));

        // 5. create features with corresponding formulas and calculationInputValues
        var resFeatures = new HashMap<Integer, List<Feature>>();
        for (var feature : features) {
            resFeatures.putIfAbsent(feature.getSequence(), new ArrayList<>());
            resFeatures.get(feature.getSequence()).add(map(feature, formulas.get(feature.getFormulaId()), calculationInputValuesMap.get(feature.getId())));
        }
        var resSequences = new HashMap<Integer, Sequence>();
        for (var entry : resFeatures.entrySet()) {
            resSequences.put(entry.getKey(), new Sequence(entry.getKey(), entry.getValue()));
        }

        // 5. create protocol
        return map(protocol, resSequences);
    }

    private ProtocolDTO get(long protocolId) throws ProtocolUnresolvableException {
        try {
            var res = restTemplate.getForObject(UrlFactory.protocol(protocolId), ProtocolDTO.class);
            if (res == null) {
                throw new ProtocolUnresolvableException("Protocol could not be converted");
            }
            return res;
        } catch (HttpClientErrorException.NotFound ex) {
            throw new ProtocolUnresolvableException("Protocol not found");
        } catch (HttpClientErrorException ex) {
            throw new ProtocolUnresolvableException("Error while fetching protocol");
        }
    }

    private List<FeatureDTO> getFeatures(long protocolId) throws ProtocolUnresolvableException {
        try {
            var res = restTemplate.getForObject(UrlFactory.protocolFeatures(protocolId), FeatureDTO[].class);
            if (res == null) {
                throw new ProtocolUnresolvableException("Features could not be converted");
            }
            return Arrays.asList(res);
        } catch (HttpClientErrorException.NotFound ex) {
            throw new ProtocolUnresolvableException("Features not found");
        } catch (HttpClientErrorException ex) {
            throw new ProtocolUnresolvableException("Error while fetching features");
        }
    }

    private List<CalculationInputValueDTO> getCivs(long protocolId) throws ProtocolUnresolvableException {
        try {
            var res = restTemplate.getForObject(UrlFactory.protocolCiv(protocolId), CalculationInputValueDTO[].class);
            if (res == null) {
                throw new ProtocolUnresolvableException("Civs could not be converted");
            }
            return Arrays.asList(res);
        } catch (HttpClientErrorException.NotFound ex) {
            throw new ProtocolUnresolvableException("Civs not found");
        } catch (HttpClientErrorException ex) {
            throw new ProtocolUnresolvableException("Error while fetching Civs");
        }
    }


    private CalculationInputValue map(CalculationInputValueDTO calculationInputValueDTO, CalculationInputValue calculationInputValue) {
        modelMapper.map(calculationInputValueDTO, calculationInputValue);
        return calculationInputValue;
    }

    private Protocol map(ProtocolDTO p, Map<Integer, Sequence> sequences) {
        return new Protocol(
                p.getId(),
                p.getName(),
                p.getDescription(),
                p.isEditable(),
                p.isInDevelopment(),
                p.getLowWelltype(),
                p.getHighWelltype(),
                sequences);
    }

    private Feature map(FeatureDTO feature, Formula formula, List<CalculationInputValue> civs) throws ProtocolUnresolvableException {
        if (formula == null) {
            throw new ProtocolUnresolvableException(String.format("Did not found formula for feature with id %s", feature.getId()));
        }
//            if (civs == null || civs.isEmpty()) { // TODO check this logic
//            throw new ProtocolUnresolvableException(String.format("Did not found civs for feature with id %s", feature.getId()));
//        }
        return new Feature(feature.getId(),
                feature.getName(),
                feature.getAlias(),
                feature.getDescription(),
                feature.getFormat(),
                feature.getType(),
                feature.getSequence(),
                formula,
                civs);
    }

}
