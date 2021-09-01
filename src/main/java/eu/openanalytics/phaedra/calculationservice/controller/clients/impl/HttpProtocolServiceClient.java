package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ProtocolUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.CalculationInputValueDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.FeatureDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ProtocolDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.calculationservice.model.Sequence;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.calculationservice.service.ModelMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class HttpProtocolServiceClient implements ProtocolServiceClient {

    private final RestTemplate restTemplate;
    private final FormulaService formulaService;
    private final ModelMapper modelMapper;

    public HttpProtocolServiceClient(RestTemplate restTemplate, FormulaService formulaService, ModelMapper modelMapper) {
        this.restTemplate = restTemplate;
        this.formulaService = formulaService;
        this.modelMapper = modelMapper;
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
                .map(modelMapper::map)
                .collect(Collectors.groupingBy(
                        CalculationInputValue::getFeatureId,
                        Collectors.toList()
                ));

        // 5. create features with corresponding formulas and calculationInputValues
        var resFeatures = new HashMap<Integer, List<Feature>>();
        for (var featureDTO : features) {
            resFeatures.putIfAbsent(featureDTO.getSequence(), new ArrayList<>());

            var formula = formulas.get(featureDTO.getFormulaId());
            if (formula == null) {
                throw new ProtocolUnresolvableException(String.format("Did not found formula for feature with id %s", featureDTO.getId()));
            }

            var civs = calculationInputValuesMap.get(featureDTO.getId());
            if (civs == null || civs.isEmpty()) {
                throw new ProtocolUnresolvableException(String.format("Did not found civs for feature with id %s", featureDTO.getId()));
            }

            var feature = modelMapper.map(featureDTO)
                    .formula(formula)
                    .calculationInputValues(civs)
                    .build();

            resFeatures.get(feature.getSequence()).add(feature);
        }

        var resSequences = new HashMap<Integer, Sequence>();
        for (var entry : resFeatures.entrySet()) {
            resSequences.put(entry.getKey(), new Sequence(entry.getKey(), entry.getValue()));
        }

        // 5. create protocol
        return modelMapper.map(protocol)
                .sequences(resSequences)
                .build();
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


}
