package eu.openanalytics.phaedra.calculationservice.repository;

import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.model.Formula;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface FormulaRepository extends CrudRepository<Formula, Long> {

    @Query("select * from formula f where f.category = CAST(:category AS public.category) ")
    List<Formula> findFormulasByCategory(@Param("category") Category category);

}
