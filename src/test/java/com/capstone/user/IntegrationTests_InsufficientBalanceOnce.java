package com.capstone.user;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Configurations.BatchConfigBalanceErrorOnce;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.BalanceErrorOnceProcessor;
import com.capstone.user.Readers.UserTransactionReaderCSV;
import com.capstone.user.TaskExecutors.TaskExecutor;
import com.capstone.user.Writers.UserTransactionCompositeWriter;
import org.aspectj.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.File;

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigBalanceErrorOnce.class,
        UserTransactionClassifier.class,
        UserTransactionModel.class,
        UserTransactionReaderCSV.class,
        BalanceErrorOnceProcessor.class,
        UserTransactionCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_InsufficientBalanceOnce {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Hardcoded userID - matches first userID in test_input_insufficientBalance.csv source
    private long userID = 0L;
    private String INPUT = "src/test/resources/input/test_input_insufficientBalance.csv";
    private String EXPECTED_OUTPUT = "src/test/resources/output/expected_output_insufficientBalance.xml";
    private String ACTUAL_OUTPUT = "src/test/resources/output/insufficient_balance_once";

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_BalanceErrorOnce() {

        return new JobParametersBuilder()
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }

    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_BalanceErrorOnce() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_BalanceErrorOnce());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileExpected = new File(EXPECTED_OUTPUT);
        File testOutputFileActual = new File(ACTUAL_OUTPUT + "/user_" + userID + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("balanceErrorOnceJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (expected) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected));

        // Verify output (actual) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual));
    }
}
