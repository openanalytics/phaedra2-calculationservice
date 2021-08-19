package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.calculationservice.model.Protocol;

import java.util.Optional;

public interface ProtocolServiceClient {

    Optional<Protocol> getProtocol(long protocolId);

}