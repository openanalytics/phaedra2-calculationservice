/**
 * Phaedra II
 *
 * Copyright (C) 2016-2023 Open Analytics
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
package eu.openanalytics.phaedra.calculationservice;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import com.fasterxml.jackson.core.type.TypeReference;

import eu.openanalytics.phaedra.calculationservice.dto.FormulaDTO;
import eu.openanalytics.phaedra.calculationservice.enumeration.CalculationScope;
import eu.openanalytics.phaedra.calculationservice.enumeration.Category;
import eu.openanalytics.phaedra.calculationservice.enumeration.ScriptLanguage;
import eu.openanalytics.phaedra.calculationservice.service.FormulaService;
import eu.openanalytics.phaedra.calculationservice.support.AbstractIntegrationTest;

@Disabled
public class FormulaIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private FormulaService formulaService;

    @Test
    public void simpleCreateAndGetTest() throws Exception {
        // 1. create simple formula
        var input1 = FormulaDTO.builder()
                .formula("output <- input * 2")
                .name("MyFormula")
                .description("A description")
                .category(Category.CALCULATION)
                .language(ScriptLanguage.R)
                .scope(CalculationScope.WELL)
                .versionNumber("1.0")
                .build();

        var res1 = performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        Assertions.assertEquals(1, res1.getId());
        Assertions.assertEquals("output <- input * 2", res1.getFormula());
        Assertions.assertEquals("MyFormula", res1.getName());
        Assertions.assertEquals("A description", res1.getDescription());
        Assertions.assertEquals(Category.CALCULATION, res1.getCategory());
        Assertions.assertEquals(ScriptLanguage.R, res1.getLanguage());
        Assertions.assertEquals(CalculationScope.WELL, res1.getScope());
        Assertions.assertEquals("1.0", res1.getVersionNumber().split("-")[0]);
        Assertions.assertEquals(null, res1.getPreviousVersion());

        // 2. get formula
        var res2 = performRequest(get("/formulas/1"), HttpStatus.OK, FormulaDTO.class);
        Assertions.assertEquals(1, res2.getId());
        Assertions.assertEquals("output <- input * 2", res2.getFormula());
        Assertions.assertEquals("MyFormula", res2.getName());
        Assertions.assertEquals("A description", res2.getDescription());
        Assertions.assertEquals(Category.CALCULATION, res2.getCategory());
        Assertions.assertEquals(ScriptLanguage.R, res2.getLanguage());
        Assertions.assertEquals(CalculationScope.WELL, res2.getScope());
        Assertions.assertEquals("1.0", res2.getVersionNumber().split("-")[0]);
        Assertions.assertEquals(null, res2.getPreviousVersion());
        Assertions.assertEquals("Anonymous", res2.getCreatedBy());
        Assertions.assertEquals("2042-12-31T23:59:59", res2.getCreatedOn().toString());
        Assertions.assertNull(res2.getUpdatedBy());
        Assertions.assertNull(res2.getUpdatedOn());

        // 3. Delete formula
        performRequest(delete("/formulas/1"), HttpStatus.NO_CONTENT);

        // 4. get formula again
        var res4 = performRequest(get("/formulas/1"), HttpStatus.NOT_FOUND);
        Assertions.assertEquals("{\"error\":\"Requested formula with id 1 was not found\",\"status\":\"error\"}", res4);
    }

    @Test
    public void testDeleteNonExistingFormula() throws Exception {
        // 3. Delete formula
        var res1 = performRequest(delete("/formulas/1"), HttpStatus.NOT_FOUND);

        Assertions.assertEquals("{\"error\":\"Requested formula with id 1 was not found\",\"status\":\"error\"}", res1);
    }

    @Test
    public void testUpdateNonExistingFormula() throws Exception {
        // 3. Delete formula
        var input1 = FormulaDTO.builder()
                .formula("output <- input * 3")
                .name("MyFormula_updated")
                .description("An updated description")
                .category(Category.HIT_CALLING)
                .language(ScriptLanguage.JAVASCRIPT)
                .scope(CalculationScope.PLATE)
                .build();

        var res1 = performRequest(put("/formulas/1", input1), HttpStatus.NOT_FOUND);

        Assertions.assertEquals("{\"error\":\"Requested formula with id 1 was not found\",\"status\":\"error\"}", res1);
    }

    @Test
    public void testUpdateFormula() throws Exception {
        // 1. create simple formula
        var input1 = FormulaDTO.builder()
                .formula("output <- input * 2")
                .name("MyFormula")
                .description("A description")
                .category(Category.CALCULATION)
                .language(ScriptLanguage.R)
                .scope(CalculationScope.WELL)
                .versionNumber("1.0")
                .build();

        var res1 = performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        Assertions.assertEquals(1, res1.getId());
        Assertions.assertEquals("output <- input * 2", res1.getFormula());
        Assertions.assertEquals("MyFormula", res1.getName());
        Assertions.assertEquals("A description", res1.getDescription());
        Assertions.assertEquals(Category.CALCULATION, res1.getCategory());
        Assertions.assertEquals(ScriptLanguage.R, res1.getLanguage());
        Assertions.assertEquals(CalculationScope.WELL, res1.getScope());

        // 2. update formula
        var input2 = FormulaDTO.builder()
                .formula("output <- input * 3")
                .name("MyFormula_updated")
                .description("An updated description")
                .category(Category.HIT_CALLING)
                .language(ScriptLanguage.JAVASCRIPT)
                .scope(CalculationScope.PLATE)
                .versionNumber("2.0")
                .previousVersion(res1.getVersionNumber())
                .build();

        var res2 = performRequest(put("/formulas/1", input2), HttpStatus.OK, FormulaDTO.class);
        Assertions.assertEquals(2, res2.getId());
        Assertions.assertEquals("output <- input * 3", res2.getFormula());
        Assertions.assertEquals("MyFormula_updated", res2.getName());
        Assertions.assertEquals("An updated description", res2.getDescription());
        Assertions.assertEquals(Category.HIT_CALLING, res2.getCategory());
        Assertions.assertEquals(ScriptLanguage.JAVASCRIPT, res2.getLanguage());
        Assertions.assertEquals(CalculationScope.PLATE, res2.getScope());
        Assertions.assertEquals(res1.getVersionNumber(), res2.getPreviousVersion());
        Assertions.assertEquals("2.0", res2.getVersionNumber().split("-")[0]);
        Assertions.assertEquals("Anonymous", res2.getCreatedBy());
        Assertions.assertEquals("2042-12-31T23:59:59", res2.getCreatedOn().toString());
        Assertions.assertEquals("Anonymous", res2.getUpdatedBy());
        Assertions.assertEquals("2042-12-31T23:59:59", res2.getUpdatedOn().toString());

        // 3. get formula
        var res3 = performRequest(get("/formulas/2"), HttpStatus.OK, FormulaDTO.class);
        Assertions.assertEquals(2, res3.getId());
        Assertions.assertEquals("output <- input * 3", res2.getFormula());
        Assertions.assertEquals("MyFormula_updated", res2.getName());
        Assertions.assertEquals("An updated description", res2.getDescription());
        Assertions.assertEquals(Category.HIT_CALLING, res2.getCategory());
        Assertions.assertEquals(ScriptLanguage.JAVASCRIPT, res2.getLanguage());
        Assertions.assertEquals(CalculationScope.PLATE, res2.getScope());
        Assertions.assertEquals(res1.getVersionNumber(), res3.getPreviousVersion());
        Assertions.assertEquals("2.0", res3.getVersionNumber().split("-")[0]);
        Assertions.assertEquals("Anonymous", res2.getCreatedBy());
        Assertions.assertEquals("2042-12-31T23:59:59", res2.getCreatedOn().toString());
        Assertions.assertEquals("Anonymous", res2.getUpdatedBy());
        Assertions.assertEquals("2042-12-31T23:59:59", res2.getUpdatedOn().toString());
    }

    @Test
    public void testGetRequests() throws Exception {
        // 1. create a few formulas...
        for (long i = 1; i <= 5; i++) {
            var input1 = FormulaDTO.builder()
                    .formula("output <- input * 2 + " + i)
                    .name("MyFormula " + i)
                    .description("A description")
                    .category(Category.CALCULATION)
                    .language(ScriptLanguage.R)
                    .scope(CalculationScope.WELL)
                    .versionNumber("1.0")
                    .build();
            performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        }
        for (long i = 1; i <= 5; i++) {
            var input1 = FormulaDTO.builder()
                    .formula("output <- input * 2 + " + i)
                    .name("MyFormula " + i)
                    .description("A description")
                    .category(Category.HIT_CALLING)
                    .language(ScriptLanguage.R)
                    .scope(CalculationScope.WELL)
                    .versionNumber("1.0")
                    .build();
            performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        }
        for (long i = 1; i <= 5; i++) {
            var input1 = FormulaDTO.builder()
                    .formula("output <- input * 2 + " + i)
                    .name("MyFormula " + i)
                    .description("A description")
                    .category(Category.OUTLIER_DETECTION)
                    .language(ScriptLanguage.R)
                    .scope(CalculationScope.WELL)
                    .versionNumber("1.0")
                    .build();
            performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        }

        var t = new TypeReference<List<FormulaDTO>>() {
        };

        // 2. query formulas
        var res2 = performRequest(get("/formulas"), HttpStatus.OK, t);
        Assertions.assertEquals(15, res2.size());
        Assertions.assertEquals("output <- input * 2 + 1", res2.get(0).getFormula());
        Assertions.assertEquals(Category.CALCULATION, res2.get(0).getCategory());
        Assertions.assertEquals("output <- input * 2 + 5", res2.get(9).getFormula());
        Assertions.assertEquals(Category.HIT_CALLING, res2.get(9).getCategory());
        Assertions.assertEquals("output <- input * 2 + 5", res2.get(14).getFormula());
        Assertions.assertEquals(Category.OUTLIER_DETECTION, res2.get(14).getCategory());

        // 3. query by category
        var res3 = performRequest(get("/formulas?category=CALCULATION"), HttpStatus.OK, t);
        Assertions.assertEquals(5, res3.size());
        Assertions.assertEquals(5L, res3.stream().filter((f) -> f.getCategory() == Category.CALCULATION).count());

        var res4 = performRequest(get("/formulas?category=HIT_CALLING"), HttpStatus.OK, t);
        Assertions.assertEquals(5, res4.size());
        Assertions.assertEquals(5L, res4.stream().filter((f) -> f.getCategory() == Category.HIT_CALLING).count());

        var res5 = performRequest(get("/formulas?category=OUTLIER_DETECTION"), HttpStatus.OK, t);
        Assertions.assertEquals(5, res5.size());
        Assertions.assertEquals(5L, res5.stream().filter((f) -> f.getCategory() == Category.OUTLIER_DETECTION).count());
    }


    @Test
    public void testGetByIds() throws Exception {
        // 1. create a few formulas...
        for (long i = 1; i <= 5; i++) {
            var input1 = FormulaDTO.builder()
                    .formula("output <- input * 2 + " + i)
                    .name("MyFormula " + i)
                    .description("A description")
                    .category(Category.CALCULATION)
                    .language(ScriptLanguage.R)
                    .scope(CalculationScope.WELL)
                    .versionNumber("1.0")
                    .build();
            performRequest(post("/formulas", input1), HttpStatus.CREATED, FormulaDTO.class);
        }

        var res = formulaService.getFormulasByIds(List.of(1L, 4L, 5L));
        Assertions.assertEquals(3, res.size());
        Assertions.assertEquals(1, res.get(1L).getId());
        Assertions.assertEquals(4, res.get(4L).getId());
        Assertions.assertEquals(5, res.get(5L).getId());

        var res2 = formulaService.getFormulasByIds(List.of(1L, 42L));
        Assertions.assertEquals(1, res2.size());
        Assertions.assertEquals(1, res.get(1L).getId());
        Assertions.assertNull(res.get(42L));
    }

    @Test
    public void invalidJsonTest() throws Exception {
        var res1 = performRequest(
                MockMvcRequestBuilders.post("/formulas")
                        .contentType("application/json")
                        .content("{\"test"),
                HttpStatus.BAD_REQUEST);

        Assertions.assertEquals("{\"error\":\"Validation error\",\"status\":\"error\"}", res1);
    }

    @Test
    public void testCreationValidationTest() throws Exception {
        // 1. missing fields
        var input1 = FormulaDTO.builder()
                .build();

        var res1 = performRequest(post("/formulas", input1), HttpStatus.BAD_REQUEST);
        Assertions.assertEquals("{\"error\":\"Validation error\",\"malformed_fields\":{\"category\":\"Category is mandatory\",\"formula\":\"Formula is mandatory\",\"language\":\"Language is mandatory\",\"name\":\"Name is mandatory\",\"scope\":\"Scope is mandatory\",\"versionNumber\":\"versionNumber is mandatory\"},\"status\":\"error\"}", res1);

        // 2. too many fields
        var input2 = FormulaDTO.builder()
                .formula("output <- input * 2")
                .name("MyFormula")
                .description("A description")
                .category(Category.CALCULATION)
                .language(ScriptLanguage.R)
                .scope(CalculationScope.WELL)
                .createdBy("Alice")
                .createdOn(LocalDateTime.now())
                .updatedBy("Alice")
                .updatedOn(LocalDateTime.now())
                .build();

        var res2 = performRequest(post("/formulas", input2), HttpStatus.BAD_REQUEST);
        Assertions.assertEquals("{\"error\":\"Validation error\",\"malformed_fields\":{\"createdBy\":\"CreatedBy must be null when creating a formula\",\"createdOn\":\"CreatedOn must be null when creating a formula\",\"updatedBy\":\"UpdatedBy must be null when creating a formula\",\"updatedOn\":\"UpdatedOn must be null when creating a formula\",\"versionNumber\":\"versionNumber is mandatory\"},\"status\":\"error\"}", res2);

        // 3. wrong category
        var res3 = performRequest(post("/formulas", new HashMap<>() {{
            put("formula", "output <- input * 2");
            put("name", "MyFormula");
            put("description", "A description");
            put("category", "NOT_EXISTING_CATEGORY");
            put("scope", "WELL");
            put("language", "R");
        }}), HttpStatus.BAD_REQUEST);

        Assertions.assertEquals("{\"error\":\"Validation error\",\"malformed_fields\":{\"category\":\"Invalid value (\\\"NOT_EXISTING_CATEGORY\\\") provided\"},\"status\":\"error\"}", res3);

        // 4. wrong scope
        var res4 = performRequest(post("/formulas", new HashMap<>() {{
            put("formula", "output <- input * 2");
            put("name", "MyFormula");
            put("description", "A description");
            put("category", "CALCULATION");
            put("scope", "NOT_EXISTING_SCOPE");
            put("language", "R");
        }}), HttpStatus.BAD_REQUEST);

        Assertions.assertEquals("{\"error\":\"Validation error\",\"malformed_fields\":{\"scope\":\"Invalid value (\\\"NOT_EXISTING_SCOPE\\\") provided\"},\"status\":\"error\"}", res4);

        // 5. wrong language
        var res5 = performRequest(post("/formulas", new HashMap<>() {{
            put("formula", "output <- input * 2");
            put("name", "MyFormula");
            put("description", "A description");
            put("category", "CALCULATION");
            put("scope", "WELL");
            put("language", "COBOL");
        }}), HttpStatus.BAD_REQUEST);

        Assertions.assertEquals("{\"error\":\"Validation error\",\"malformed_fields\":{\"language\":\"Invalid value (\\\"COBOL\\\") provided\"},\"status\":\"error\"}", res5);
    }

}
