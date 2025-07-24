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
package eu.openanalytics.phaedra.calculationservice.api;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.FormulaCategory;
import eu.openanalytics.phaedra.calculationservice.exception.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.util.exceptionhandling.HttpMessageNotReadableExceptionHandler;
import eu.openanalytics.phaedra.util.exceptionhandling.MethodArgumentNotValidExceptionHandler;
import eu.openanalytics.phaedra.util.exceptionhandling.UserVisibleExceptionHandler;

@RestController
@RequestMapping("/formulas")
@Validated
public class FormulaController implements MethodArgumentNotValidExceptionHandler, HttpMessageNotReadableExceptionHandler, UserVisibleExceptionHandler {

    private final FormulaService formulaService;

    public FormulaController(FormulaService formulaService) {
        this.formulaService = formulaService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public FormulaDTO createFormula(@Validated(OnCreate.class) @RequestBody FormulaDTO formulaDTO) {
        return formulaService.createFormula(formulaDTO);
    }

    @PutMapping("/{formulaId}")
    public FormulaDTO updateFormula(@Validated(OnUpdate.class) @RequestBody FormulaDTO formulaDTO, @PathVariable long formulaId) throws FormulaNotFoundException {
        return formulaService.updateFormula(formulaId, formulaDTO);
    }
    
    @PutMapping("/{formulaId}/status")
    public FormulaDTO updateFormulaStatus(@RequestBody FormulaDTO formulaDTO, @PathVariable long formulaId) throws FormulaNotFoundException {
        return formulaService.updateFormulaStatus(formulaId, formulaDTO);
    }

    @DeleteMapping("/{formulaId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteFormula(@PathVariable long formulaId) throws FormulaNotFoundException {
        formulaService.deleteFormula(formulaId);
    }

    @GetMapping("/{formulaId}")
    public FormulaDTO getFormula(@PathVariable long formulaId) throws FormulaNotFoundException {
        return formulaService.getFormulaById(formulaId);
    }

    @GetMapping
    public List<FormulaDTO> getAllFormulas() {
        return formulaService.getAllFormulas();
    }

    @GetMapping(params = {"category"})
    public List<FormulaDTO> getFormulasByCategory(@RequestParam(value = "category", required = false) FormulaCategory category) {
        return formulaService.getFormulasByCategory(category);
    }

    @GetMapping("/{formulaId}/inputs")
    public List<String> getFormulaInputNames(@PathVariable long formulaId) throws FormulaNotFoundException {
        return formulaService.getFormulaInputNames(formulaId);
    }
}
