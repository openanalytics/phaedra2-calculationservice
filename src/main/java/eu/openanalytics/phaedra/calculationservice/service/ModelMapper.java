package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.CalculationInputValueDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.FeatureDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ProtocolDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import org.modelmapper.Conditions;
import org.modelmapper.config.Configuration;
import org.modelmapper.convention.NameTransformers;
import org.modelmapper.convention.NamingConventions;
import org.springframework.stereotype.Service;

@Service
public class ModelMapper {

    private final org.modelmapper.ModelMapper modelMapper = new org.modelmapper.ModelMapper();

    public ModelMapper() {
        modelMapper.typeMap(FormulaDTO.class, Formula.class);
        modelMapper.typeMap(Formula.class, FormulaDTO.class);

        Configuration builderConfiguration = modelMapper.getConfiguration().copy()
                .setDestinationNameTransformer(NameTransformers.builder())
                .setDestinationNamingConvention(NamingConventions.builder());

        modelMapper.createTypeMap(FormulaDTO.class, Formula.FormulaBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.createTypeMap(CalculationInputValueDTO.class, CalculationInputValue.CalculationInputValueBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.validate(); // ensure that objects can be mapped
    }

    public Formula.FormulaBuilder map(FormulaDTO formulaDTO, Formula formula) {
        var builder = formula.toBuilder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    public Formula.FormulaBuilder map(FormulaDTO formulaDTO) {
        var builder = Formula.builder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    public FormulaDTO map(Formula formula) {
        var res = new FormulaDTO();
        modelMapper.map(formula, res);
        return res;
    }

    public CalculationInputValue map(CalculationInputValueDTO calculationInputValueDTO, CalculationInputValue calculationInputValue) {
        var builder = calculationInputValue.toBuilder();
        modelMapper.map(calculationInputValueDTO, builder);
        return builder.build();
    }

    public CalculationInputValue map(CalculationInputValueDTO calculationInputValueDTO) {
        return modelMapper.map(calculationInputValueDTO, CalculationInputValue.CalculationInputValueBuilder.class).build();
    }

    public Protocol.ProtocolBuilder map(ProtocolDTO protocolDTO) {
        var builder = Protocol.builder();
        modelMapper.map(protocolDTO, builder);
        return builder;
    }

    public Feature.FeatureBuilder map(FeatureDTO featureDTO) {
        var builder = Feature.builder();
        modelMapper.map(featureDTO, builder);
        return builder;
    }



}
