package eu.openanalytics.phaedra.calculationservice.model;

import lombok.Value;

import java.util.List;

@Value
public class Protocol {

    Long id;

    String name;

    String description;

    boolean editable;

    boolean inDevelopment;

    String lowWelltype;

    String highWelltype;

    List<Feature> features;
}
