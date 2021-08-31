package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.repository.FormulaRepository;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class FormulaService {
    private final ModelMapper modelMapper = new ModelMapper();

    private final FormulaRepository formulaRepository;

    public FormulaService(FormulaRepository formulaRepository) {
        this.formulaRepository = formulaRepository;
        modelMapper.typeMap(FormulaDTO.class, Formula.class);
        modelMapper.typeMap(Formula.class, FormulaDTO.class);
        modelMapper.validate(); // ensure that objects can be mapped
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        Formula formula = Formula.builder()
                .id(formulaDTO.getId())
                .name(formulaDTO.getName())
                .description(formulaDTO.getDescription())
                .category(formulaDTO.getCategory())
                .formula(formulaDTO.getFormula())
                .language(formulaDTO.getLanguage())
                .scope(formulaDTO.getScope())
                .createdBy("Anonymous") // TODO
                .createdOn(LocalDateTime.now())
                .build();

        return save(formula);
    }

    public FormulaDTO updateFormula(FormulaDTO formulaDTO) throws FormulaNotFoundException {
        Optional<Formula> existingFormula = formulaRepository.findById(formulaDTO.getId());
        if (existingFormula.isEmpty()) {
            throw new FormulaNotFoundException(formulaDTO.getId());
        }

        Formula updatedFormula = map(formulaDTO, existingFormula.get())
                .withUpdatedBy("Anonymous") // TODO
                .withUpdatedOn(LocalDateTime.now());
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

        return map(formula.get(), new FormulaDTO());
    }

    public List<FormulaDTO> getAllFormulas() {
        return ((List<Formula>) formulaRepository.findAll())
                .stream()
                .map(f -> map(f, new FormulaDTO()))
                .collect(Collectors.toList());
    }

    public List<FormulaDTO> getFormulasByCategory(Category category) {
        return formulaRepository.findFormulasByCategory(category)
                .stream()
                .map(f -> map(f, new FormulaDTO()))
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

    private FormulaDTO map(Formula formula, FormulaDTO formulaDTO) {
        modelMapper.typeMap(Formula.class, FormulaDTO.class).map(formula, formulaDTO);
        return formulaDTO;
    }

    private Formula map(FormulaDTO formulaDTO, Formula formula) {
        modelMapper.typeMap(FormulaDTO.class, Formula.class)
                .map(formulaDTO, formula);
        return formula;
    }

    private FormulaDTO save(Formula formula) {
        Formula newFormula = formulaRepository.save(formula);

        FormulaDTO result = new FormulaDTO();
        modelMapper.typeMap(Formula.class, FormulaDTO.class).map(newFormula, result);

        return result;
    }

}
