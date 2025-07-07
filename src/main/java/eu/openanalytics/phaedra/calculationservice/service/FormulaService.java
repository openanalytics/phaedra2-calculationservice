/**
 * Phaedra II
 *
 * Copyright (C) 2016-2025 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice.service;


import eu.openanalytics.phaedra.calculationservice.dto.PropertyDTO;
import eu.openanalytics.phaedra.metadataservice.client.MetadataServiceGraphQlClient;
import eu.openanalytics.phaedra.metadataservice.dto.MetadataDTO;
import eu.openanalytics.phaedra.metadataservice.dto.TagDTO;
import eu.openanalytics.phaedra.metadataservice.enumeration.ObjectClass;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.exception.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.model.ModelMapper;
import eu.openanalytics.phaedra.calculationservice.repository.FormulaRepository;
import eu.openanalytics.phaedra.calculationservice.util.FormulaParser;
import eu.openanalytics.phaedra.util.auth.IAuthorizationService;
import eu.openanalytics.phaedra.util.versioning.VersionUtils;

@Service
public class FormulaService {

    private final FormulaRepository formulaRepository;
    private final ModelMapper modelMapper;
    private final Clock clock;
    private final IAuthorizationService authService;
    private final MetadataServiceGraphQlClient metadataServiceGraphQlClient;

    public FormulaService(FormulaRepository formulaRepository, ModelMapper modelMapper, Clock clock,
        IAuthorizationService authService, MetadataServiceGraphQlClient metadataServiceGraphQlClient) {
        this.formulaRepository = formulaRepository;
        this.modelMapper = modelMapper;
        this.clock = clock;
        this.authService = authService;
        this.metadataServiceGraphQlClient = metadataServiceGraphQlClient;
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        var formula = modelMapper.map(formulaDTO)
        		.versionNumber(VersionUtils.generateNewVersion(null, false))
        		.category(FormulaCategory.CALCULATION)
                .createdBy(authService.getCurrentPrincipalName())
                .createdOn(LocalDateTime.now(clock))
                .build();
        return save(formula);
    }

    public FormulaDTO updateFormula(long formulaId, FormulaDTO formulaDTO) throws FormulaNotFoundException {
        Optional<Formula> existingFormula = formulaRepository.findById(formulaId);
        if (existingFormula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }
        authService.performOwnershipCheck(existingFormula.get().getCreatedBy());

        LocalDateTime date = LocalDateTime.now(clock);
        Formula previousFormula = existingFormula.get();
        Long previousFormulaId = previousFormula.getId();

        Formula updatedFormula = modelMapper.map(formulaDTO, previousFormula)
                .id(null) //To create new formula
                .versionNumber(VersionUtils.generateNewVersion(formulaDTO.getVersionNumber(), false))
                .previousVersionId(previousFormulaId)
                .updatedBy(authService.getCurrentPrincipalName())
                .updatedOn(date)
                .build();
        return save(updatedFormula);
    }

    public void deleteFormula(long formulaId) throws FormulaNotFoundException {
        Optional<Formula> formula = formulaRepository.findById(formulaId);
        if (formula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }
        authService.performOwnershipCheck(formula.get().getCreatedBy());
       	formulaRepository.deleteById(formulaId);
    }

    public FormulaDTO getFormulaById(long formulaId) throws FormulaNotFoundException {
        Optional<Formula> formula = formulaRepository.findById(formulaId);
        if (formula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }

        return modelMapper.map(formula.get()).build();
    }

    public List<FormulaDTO> getAllFormulas() {
        List<Formula> formulas = (List<Formula>) formulaRepository.findAll();
        List<FormulaDTO> formulaDTOs = formulas.stream()
            .map((f) -> modelMapper.map(f).build())
            .collect(Collectors.toList());

        enrichWithMetadata(formulaDTOs);

        return formulaDTOs;

    }

    public List<FormulaDTO> getFormulasByCategory(FormulaCategory category) {
        List<Formula> formulas = formulaRepository.findFormulasByCategory(category);
        List<FormulaDTO> formulaDTOs = formulas.stream()
                .map((f) -> modelMapper.map(f).build())
                .collect(Collectors.toList());

        enrichWithMetadata(formulaDTOs);

        return formulaDTOs;
    }

    public Map<Long, Formula> getFormulasByIds(List<Long> formulaIds) {
        return ((List<Formula>) formulaRepository.findAllById(formulaIds))
                .stream()
                .collect(Collectors.toMap(Formula::getId, (f) -> f));
    }

    public List<String> getFormulaInputNames(long formulaId) throws FormulaNotFoundException {
    	Optional<Formula> formula = formulaRepository.findById(formulaId);
        if (formula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }
    	return new FormulaParser().parseInputNames(formula.get());
    }

    private FormulaDTO save(Formula formula) {
        Formula newFormula = formulaRepository.save(formula);
        return modelMapper.map(newFormula).build();
    }

    private void enrichWithMetadata(List<FormulaDTO> formulas) {
        if (CollectionUtils.isNotEmpty(formulas)) {
            // Create a map of plate ID to PlateDTO for quick lookup
            Map<Long, FormulaDTO> formulaMap = new HashMap<>();
            List<Long> formulaIds = new ArrayList<>(formulas.size());
            for (FormulaDTO formula : formulas) {
                formulaMap.put(formula.getId(), formula);
                formulaIds.add(formula.getId());
            }

            // Retrieve the metadata using the list of plate IDs
            List<MetadataDTO> formulaMetadata = metadataServiceGraphQlClient
                .getMetadata(formulaIds, ObjectClass.FORMULA);

            for (MetadataDTO metadata : formulaMetadata) {
                FormulaDTO formula = formulaMap.get(metadata.getObjectId());
                if (formula != null) {
                    formula.setTags(metadata.getTags().stream()
                        .map(TagDTO::getTag)
                        .toList());
                    List<PropertyDTO> propertyDTOs = new ArrayList<>(metadata.getProperties().size());
                    for (eu.openanalytics.phaedra.metadataservice.dto.PropertyDTO property : metadata.getProperties()) {
                        propertyDTOs.add(new PropertyDTO(property.getPropertyName(), property.getPropertyValue()));
                    }
                    formula.setProperties(propertyDTOs);
                }
            }
        }
    }
}
