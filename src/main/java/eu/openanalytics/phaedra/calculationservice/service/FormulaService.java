package eu.openanalytics.phaedra.calculationservice.service;


import java.time.Clock;
import java.time.LocalDateTime;
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

@Service
public class FormulaService {

    private final FormulaRepository formulaRepository;
    private final ModelMapper modelMapper;
    private final Clock clock;

    public FormulaService(FormulaRepository formulaRepository, ModelMapper modelMapper, Clock clock) {
        this.formulaRepository = formulaRepository;
        this.modelMapper = modelMapper;
        this.clock = clock;
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        var formula = modelMapper.map(formulaDTO)
                .createdBy("Anonymous")
                .createdOn(LocalDateTime.now(clock))
                .build();

        return save(formula);
    }

    public FormulaDTO updateFormula(long formulaId, FormulaDTO formulaDTO) throws FormulaNotFoundException {
        Optional<Formula> existingFormula = formulaRepository.findById(formulaId);
        if (existingFormula.isEmpty()) {
            throw new FormulaNotFoundException(formulaId);
        }

        Formula updatedFormula = modelMapper.map(formulaDTO, existingFormula.get())
                .updatedBy("Anonymous")
                .updatedOn(LocalDateTime.now(clock))
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
