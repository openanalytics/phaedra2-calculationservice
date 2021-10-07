package eu.openanalytics.phaedra.calculationservice.model;

import java.util.Optional;

/**
 * TODO
 */
public class SuccessTracker<T> {

    private boolean success = true;

    private Optional<T> result = Optional.empty();

    public void failedIfStepFailed(SuccessTracker<?> success) {
        if (!success.isSuccess()) {
            this.success = false;
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public void failed() {
        success = false;
    }

    public void setResult(T result) {
        this.result = Optional.of(result);
    }

    public T getResult() {
        return result.get();
    }

}


