package com.capstone.user;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Configurations.BatchConfigBalanceErrorMany;
import com.capstone.user.Controllers.UserTransactionController;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.BalanceErrorManyProcessor;
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
        BatchConfigBalanceErrorMany.class,
        UserTransactionClassifier.class,
        UserTransactionModel.class,
        UserTransactionReaderCSV.class,
        BalanceErrorManyProcessor.class,
        UserTransactionCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_InsufficientBalanceMany {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Hardcoded userIDs - matches first and second userID in test_input_insufficientBalanceMany.csv source
    // (matched userIDs are expected IDs to return multiple balance errors)
    private long userID_first = 0L;
    private long userID_second = 41L;
    private String INPUT = "src/test/resources/input/test_input_insufficientBalanceMany.csv";
    private String ACTUAL_OUTPUT = "src/test/resources/output/insufficient_balance_many";

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_BalanceErrorMany() {

        return new JobParametersBuilder()
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_BalanceErrorMany() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_BalanceErrorMany());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileActual_1 = new File(ACTUAL_OUTPUT + "/user_" + userID_first + "_transactions.xml");
        File testOutputFileActual_2 = new File(ACTUAL_OUTPUT + "/user_" + userID_second + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("balanceErrorManyJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (actual) file 1 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual_1));

        // Verify output (actual) file 2 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual_2));

    }
}
