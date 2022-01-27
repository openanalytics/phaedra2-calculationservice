package eu.openanalytics.phaedra.calculationservice.service;

import lombok.Data;
import org.springframework.stereotype.Service;

@Service
@Data
public class TokenService {
    private String authorisationToken;
}
