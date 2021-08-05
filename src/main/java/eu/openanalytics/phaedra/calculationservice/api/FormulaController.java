package eu.openanalytics.phaedra.calculationservice.api;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/formulas")
@Slf4j
public class FormulaController {

    @Autowired
    private FormulaService formulaService;

    @PostMapping
    public FormulaDTO createFormula(@RequestBody FormulaDTO formulaDTO) {
        return formulaService.createFormula(formulaDTO);
    }

    @PutMapping
    public FormulaDTO updateFormula(@RequestBody FormulaDTO formulaDTO) {
        return formulaService.updateFormula(formulaDTO);
    }

    @DeleteMapping("/{formulaId}")
    public ResponseEntity deleteFormula(@PathVariable long formulaId) {
        formulaService.deleteFormula(formulaId);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{formulaId}")
    public FormulaDTO getFormulas(@PathVariable long formulaId) {
        return formulaService.getFormulaById(formulaId);
    }

    @GetMapping
    public List<FormulaDTO> getFormulas() {
        return formulaService.getAllFormulas();
    }

    @GetMapping(params = {"category"})
    public List<FormulaDTO> getFormulasByCategory(@RequestParam(value = "category", required = false) Category category) {
        return formulaService.getFormulasByCategory(category);
    }
}
