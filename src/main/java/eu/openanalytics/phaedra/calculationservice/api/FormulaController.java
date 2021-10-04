package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.exception.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.util.exceptionhandling.HttpMessageNotReadableExceptionHandler;
import eu.openanalytics.phaedra.util.exceptionhandling.MethodArgumentNotValidExceptionHandler;
import eu.openanalytics.phaedra.util.exceptionhandling.UserVisibleExceptionHandler;
import lombok.extern.slf4j.Slf4j;
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

import java.util.List;

@RestController
@RequestMapping("/formulas")
@Slf4j
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

    @DeleteMapping("/{formulaId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteFormula(@PathVariable long formulaId) throws FormulaNotFoundException {
        formulaService.deleteFormula(formulaId);
    }

    @GetMapping("/{formulaId}")
    public FormulaDTO getFormulas(@PathVariable long formulaId) throws FormulaNotFoundException {
        return formulaService.getFormulaById(formulaId);
    }

    @GetMapping
    public List<FormulaDTO> getFormulas() {
        return formulaService.getAllFormulas();
    }

    @GetMapping(params = {"category"})
    public List<FormulaDTO> getFormulasByCategory(@RequestParam(value = "category") Category category) {
        return formulaService.getFormulasByCategory(category);
    }

}
