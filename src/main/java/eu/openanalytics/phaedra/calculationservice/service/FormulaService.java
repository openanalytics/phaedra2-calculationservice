/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
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


import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.exception.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.repository.FormulaRepository;
import eu.openanalytics.phaedra.calculationservice.util.FormulaParser;
import eu.openanalytics.phaedra.util.auth.IAuthorizationService;

@Service
public class FormulaService {

    private final FormulaRepository formulaRepository;
    private final ModelMapper modelMapper;
    private final Clock clock;
    private final IAuthorizationService authService;

    public FormulaService(FormulaRepository formulaRepository, ModelMapper modelMapper, Clock clock, IAuthorizationService authService) {
        this.formulaRepository = formulaRepository;
        this.modelMapper = modelMapper;
        this.clock = clock;
        this.authService = authService;
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        LocalDateTime date = LocalDateTime.now(clock);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd.hhmmss");
        var formula = modelMapper.map(formulaDTO)
                .versionNumber(formulaDTO.getVersionNumber()+"-"+date.format(dateTimeFormatter))
                .createdBy(authService.getCurrentPrincipalName())
                .createdOn(date)
                .build();

        return save(formula);
    }

    public FormulaDTO updateFormula(long formulaId, FormulaDTO formulaDTO) throws FormulaNotFoundException {
        Optional<Formula> existingFormula = formulaRepository.findById(formulaId);
        if (existingFormula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }
        LocalDateTime date = LocalDateTime.now(clock);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd.hhmmss");
        Formula previousFormula = existingFormula.get();
        Formula updatedFormula = modelMapper.map(formulaDTO, previousFormula)
                .id(null) //To create new formula
                .versionNumber(formulaDTO.getVersionNumber()+"-"+date.format(dateTimeFormatter))
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
        return ((List<Formula>) formulaRepository.findAll())
                .stream()
                .map((f) -> modelMapper.map(f).build())
                .collect(Collectors.toList());
    }

    public List<FormulaDTO> getFormulasByCategory(Category category) {
        return formulaRepository.findFormulasByCategory(category)
                .stream()
                .map((f) -> modelMapper.map(f).build())
                .collect(Collectors.toList());
    }

    public Map<Long, Formula> getFormulasByIds(List<Long> formulaIds) {
        return ((List<Formula>) formulaRepository.findAllById(formulaIds))
                .stream()
                .collect(Collectors.toMap(
                        Formula::getId,
                        (f) -> f
                ));
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

}
