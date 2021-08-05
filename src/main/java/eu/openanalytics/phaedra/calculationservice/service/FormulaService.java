package eu.openanalytics.phaedra.calculationservice.service;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import eu.openanalytics.phaedra.calculationservice.repository.FormulaRepository;
import org.modelmapper.Conditions;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class FormulaService {
    private static final ModelMapper modelMapper = new ModelMapper();

    private final FormulaRepository formulaRepository;

    public FormulaService(FormulaRepository formulaRepository) {
        this.formulaRepository = formulaRepository;
    }

    public FormulaDTO createFormula(FormulaDTO formulaDTO) {
        Formula formula = new Formula();
        modelMapper.typeMap(FormulaDTO.class, Formula.class).map(formulaDTO, formula);
        Formula newFormula = formulaRepository.save(formula);

        FormulaDTO result = new FormulaDTO();
        modelMapper.typeMap(Formula.class, FormulaDTO.class).map(newFormula, result);
        return result;
    }


    public FormulaDTO updateFormula(FormulaDTO formulaDTO) {
        Optional<Formula> formula = formulaRepository.findById(formulaDTO.getId());
        formula.ifPresent(f -> {
            modelMapper.typeMap(FormulaDTO.class, Formula.class)
                    .setPropertyCondition(Conditions.isNotNull())
                    .map(formulaDTO, f);
            formulaRepository.save(f);
        });
        return formulaDTO;
    }


    public void deleteFormula(long formulaId) {
        formulaRepository.deleteById(formulaId);
    }

    public FormulaDTO getFormulaById(long formulaId) {
        Optional<Formula> formula = formulaRepository.findById(formulaId);
        FormulaDTO result = new FormulaDTO();
        formula.ifPresent(f -> modelMapper.typeMap(Formula.class, FormulaDTO.class).map(f, result));
        return result;
    }

    public List<FormulaDTO> getAllFormulas() {
        List<Formula> formulas = (List<Formula>) formulaRepository.findAll();
        return formulas.stream().map(f -> {
            FormulaDTO formulaDTO = new FormulaDTO();
            modelMapper.typeMap(Formula.class, FormulaDTO.class).map(f, formulaDTO);
            return formulaDTO;
        }).collect(Collectors.toList());
    }

    public List<FormulaDTO> getFormulasByCategory(Category category) {
        List<Formula> formulas = formulaRepository.findFormulasByCategory(category.name());
        return formulas.stream().map(f -> {
            FormulaDTO formulaDTO = new FormulaDTO();
            modelMapper.typeMap(Formula.class, FormulaDTO.class).map(f, formulaDTO);
            return formulaDTO;
        }).collect(Collectors.toList());
    }
}
