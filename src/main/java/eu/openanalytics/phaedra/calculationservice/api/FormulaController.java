package eu.openanalytics.phaedra.calculationservice.api;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnCreate;
import eu.openanalytics.phaedra.calculationservice.dto.validation.OnUpdate;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.service.FormulaNotFoundException;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/formulas")
@Slf4j
@Validated
public class FormulaController {

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

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public HashMap<String, Object> handleValidationExceptions(MethodArgumentNotValidException ex) {
        return new HashMap<>() {{
            put("status", "error");
            put("error", "Validation error");
            put("malformed_fields", ex.getBindingResult()
                    .getAllErrors()
                    .stream().
                    collect(Collectors.toMap(
                            error -> ((FieldError) error).getField(),
                            error -> Optional.ofNullable(error.getDefaultMessage()).orElse("Field is invalid"))
                    )
            );
        }};
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public HashMap<String, Object> handleValidationExceptions(HttpMessageNotReadableException ex) {
        if (ex.getCause() instanceof InvalidFormatException) {
            InvalidFormatException cause = (InvalidFormatException) ex.getCause();
            String fieldName = cause.getPath().get(0).getFieldName();

            return new HashMap<>() {{
                put("status", "error");
                put("error", "Validation error");
                put("malformed_fields", new HashMap<>() {{
                    put(fieldName, "Invalid value provided");
                }});
            }};
        }
        return new HashMap<>() {{
            put("status", "error");
            put("error", "Validation error");
        }};
    }


    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(FormulaNotFoundException.class)
    public HashMap<String, Object> handleValidationExceptions(FormulaNotFoundException ex) {
        return new HashMap<>() {{
            put("status", "error");
            put("error", ex.getMessage());
        }};
    }

}
