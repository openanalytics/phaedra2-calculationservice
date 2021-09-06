package eu.openanalytics.phaedra.calculationservice.dto.external;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.springframework.data.domain.Page;

import java.util.List;

@Value
@AllArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
public class PageDTO<T> {

    List<T> data;

    StatusDTO status;

    @Value
    @AllArgsConstructor
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE) // Jackson deserialize compatibility
    public static class StatusDTO {
        int totalPages;
        long totalElements;
        boolean first;
        boolean last;
    }

    public static <R> PageDTO<R> map(Page<R> page) {

        return new PageDTO<>(
                page.getContent(),
                new StatusDTO(
                        page.getTotalPages(),
                        page.getTotalElements(),
                        page.isFirst(),
                        page.isLast()
                )
        );

    }

}
