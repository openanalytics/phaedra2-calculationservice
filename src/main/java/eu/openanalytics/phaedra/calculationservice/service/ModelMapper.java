package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.model.CalculationInputValue;
import eu.openanalytics.phaedra.calculationservice.model.Feature;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.Protocol;
import eu.openanalytics.phaedra.protocolservice.dto.CalculationInputValueDTO;
import eu.openanalytics.phaedra.protocolservice.dto.FeatureDTO;
import eu.openanalytics.phaedra.protocolservice.dto.ProtocolDTO;
import eu.openanalytics.phaedra.resultdataservice.enumeration.StatusCode;
import eu.openanalytics.phaedra.scriptengine.dto.ResponseStatusCode;
import org.modelmapper.Conditions;
import org.modelmapper.config.Configuration;
import org.modelmapper.convention.NameTransformers;
import org.modelmapper.convention.NamingConventions;
import org.springframework.stereotype.Service;

@Service
public class ModelMapper {

    private final org.modelmapper.ModelMapper modelMapper = new org.modelmapper.ModelMapper();

    public ModelMapper() {
        Configuration builderConfiguration = modelMapper.getConfiguration().copy()
                .setDestinationNameTransformer(NameTransformers.builder())
                .setDestinationNamingConvention(NamingConventions.builder());

        modelMapper.createTypeMap(FormulaDTO.class, Formula.FormulaBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.createTypeMap(Formula.class, FormulaDTO.FormulaDTOBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.createTypeMap(CalculationInputValueDTO.class, CalculationInputValue.CalculationInputValueBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull());

        modelMapper.createTypeMap(FeatureDTO.class, Feature.FeatureBuilder.class, builderConfiguration)
                .setPropertyCondition(Conditions.isNotNull())
                .addMappings(mapper -> mapper.skip(Feature.FeatureBuilder::formula))
                .addMappings(mapper -> mapper.skip(Feature.FeatureBuilder::calculationInputValues));

        modelMapper.validate(); // ensure that objects can be mapped
    }

    /**
     * Returns a Builder that contains the properties of {@link Formula}, which are updated with the
     * values of a {@link FormulaDTO} while ignore properties in the {@link FormulaDTO} that are null.
     * The return value can be further customized by calling the builder methods.
     * This function should be used for PUT requests.
     */
    public Formula.FormulaBuilder map(FormulaDTO formulaDTO, Formula formula) {
        Formula.FormulaBuilder builder = formula.toBuilder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    /**
     * Maps a {@link FormulaDTO} to a {@link Formula.FormulaBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public Formula.FormulaBuilder map(FormulaDTO formulaDTO) {
        Formula.FormulaBuilder builder = Formula.builder();
        modelMapper.map(formulaDTO, builder);
        return builder;
    }

    /**
     * Maps a {@link Formula} to a {@link FormulaDTO.FormulaDTOBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public FormulaDTO.FormulaDTOBuilder map(Formula formula) {
        FormulaDTO.FormulaDTOBuilder builder = FormulaDTO.builder();
        modelMapper.map(formula, builder);
        return builder;
    }

    /**
     * Maps a {@link CalculationInputValueDTO} to a {@link CalculationInputValue.CalculationInputValueBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public CalculationInputValue.CalculationInputValueBuilder map(CalculationInputValueDTO calculationInputValueDTO) {
        return modelMapper.map(calculationInputValueDTO, CalculationInputValue.CalculationInputValueBuilder.class);
    }

    /**
     * Maps a {@link ProtocolDTO} to a {@link Protocol.ProtocolBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public Protocol.ProtocolBuilder map(ProtocolDTO protocolDTO) {
        return modelMapper.map(protocolDTO, Protocol.ProtocolBuilder.class);
    }

    /**
     * Maps a {@link FeatureDTO} to a {@link Feature.FeatureBuilder}.
     * The return value can be further customized by calling the builder methods.
     */
    public Feature.FeatureBuilder map(FeatureDTO featureDTO) {
        return modelMapper.map(featureDTO, Feature.FeatureBuilder.class);
    }

    /**
     * Maps a {@link ResponseStatusCode} to a {@link StatusCode}.
     */
    public StatusCode map(ResponseStatusCode responseStatusCode) {
        return modelMapper.map(responseStatusCode, StatusCode.class);
    }

}
