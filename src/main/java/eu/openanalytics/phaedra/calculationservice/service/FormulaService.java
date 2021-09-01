package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.repository.FormulaRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class FormulaService {

    private final FormulaRepository formulaRepository;
    private final ModelMapper modelMapper;

    public FormulaService(FormulaRepository formulaRepository, ModelMapper modelMapper) {
        this.formulaRepository = formulaRepository;
        this.modelMapper = modelMapper;
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        var formula = modelMapper.map(formulaDTO)
                .createdBy("Anonymous")
                .createdOn(LocalDateTime.now())
                .build();

        return save(formula);
    }

    public FormulaDTO updateFormula(FormulaDTO formulaDTO) throws FormulaNotFoundException {
        Optional<Formula> existingFormula = formulaRepository.findById(formulaDTO.getId());
        if (existingFormula.isEmpty()) {
            throw new FormulaNotFoundException(formulaDTO.getId());
        }

        Formula updatedFormula = modelMapper.map(formulaDTO, existingFormula.get())
                .updatedBy("Anonymous")
                .updatedOn(LocalDateTime.now())
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

        return modelMapper.map(formula.get());
    }

    public List<FormulaDTO> getAllFormulas() {
        return ((List<Formula>) formulaRepository.findAll())
                .stream()
                .map(modelMapper::map)
                .collect(Collectors.toList());
    }

    public List<FormulaDTO> getFormulasByCategory(Category category) {
        return formulaRepository.findFormulasByCategory(category)
                .stream()
                .map(modelMapper::map)
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

    private FormulaDTO save(Formula formula) {
        Formula newFormula = formulaRepository.save(formula);
        return modelMapper.map(newFormula);
    }

}
