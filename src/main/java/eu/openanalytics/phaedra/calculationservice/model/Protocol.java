package eu.openanalytics.phaedra.calculationservice.model;

import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class Protocol {

    Long id;

    String name;

    String description;

    boolean editable;

    boolean inDevelopment;

    String lowWelltype;

    String highWelltype;

    Map<Integer, Sequence> sequences;
}
