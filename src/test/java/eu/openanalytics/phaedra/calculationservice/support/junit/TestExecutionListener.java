package eu.openanalytics.phaedra.calculationservice.support.junit;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

public class TestExecutionListener extends SummaryGeneratingListener {

    public TestExecutionListener() {

    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        super.executionSkipped(testIdentifier, reason);
        if (testIdentifier == null || reason == null || !testIdentifier.isTest()) return;

        System.out.println();
        System.out.printf("\t\t--> Skipping test \"%s\"%n", testIdentifier.getDisplayName());
        System.out.println();
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        super.executionStarted(testIdentifier);
        if (testIdentifier == null || !testIdentifier.isTest()) return;

        System.out.println();
        System.out.printf("\t\t--> Started test \"%s\"%n", testIdentifier.getDisplayName());
        System.out.println();
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        super.executionFinished(testIdentifier, testExecutionResult);
        if (testIdentifier == null || testExecutionResult == null || !testIdentifier.isTest()) return;

        System.out.println();
        System.out.printf("\t\t--> Finished test \"%s\": %s%n", testIdentifier.getDisplayName(), testExecutionResult);
        System.out.println();
    }
}
