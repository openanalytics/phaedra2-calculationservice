package eu.openanalytics.phaedra.calculationservice.repository;

import eu.openanalytics.phaedra.calculationservice.model.Formula;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FormulaRepository extends CrudRepository<Formula, Long> {

    public List<Formula> findFormulasByCategory(String category);

}
