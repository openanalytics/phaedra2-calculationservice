package eu.openanalytics.phaedra.calculationservice.service;

import lombok.Data;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Data
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class TokenService {
    private String authorisationToken;
}
