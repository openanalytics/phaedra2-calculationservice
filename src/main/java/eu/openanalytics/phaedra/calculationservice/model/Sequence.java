package eu.openanalytics.phaedra.calculationservice.model;

import lombok.Value;

import java.util.List;


@Value
public class Sequence {

    int sequenceNumber;

    List<Feature> features;

}
