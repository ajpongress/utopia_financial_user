package com.capstone.user;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Configurations.BatchConfigAllUsers;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.AllUsersProcessor;
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

// ********************************************************************************
//                          Test All Users Operations
// ********************************************************************************

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigAllUsers.class,
        UserTransactionClassifier.class,
        UserTransactionModel.class,
        UserTransactionReaderCSV.class,
        AllUsersProcessor.class,
        UserTransactionCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_AllUserTransactions {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Hardcoded userID - matches first userID in test_input.csv source
    private long userID_first = 0;

    // Hardcoded userID - matches second userID in test_input.csv source
    private long userID_second = 2;

    private String INPUT = "src/test/resources/input/test_input.csv";
    private String EXPECTED_OUTPUT_1 = "src/test/resources/output/expected_output_AllUsersTransaction_1.xml";
    private String EXPECTED_OUTPUT_2 = "src/test/resources/output/expected_output_AllUsersTransaction_2.xml";
    private String ACTUAL_OUTPUT = "src/test/resources/output/users";

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_AllUsers() {

        return new JobParametersBuilder()
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }

    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_AllUsers() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_AllUsers());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileExpected_1 = new File(EXPECTED_OUTPUT_1);
        File testOutputFileExpected_2 = new File(EXPECTED_OUTPUT_2);
        File testOutputFileActual_1 = new File(ACTUAL_OUTPUT + "/user_" + userID_first + "_transactions.xml");
        File testOutputFileActual_2 = new File(ACTUAL_OUTPUT + "/user_" + userID_second + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("allUsersJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (expected) file 1 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected_1));

        // Verify output (expected) file 2 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected_2));

        // Verify output (actual) file 1 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual_1));

        // Verify output (actual) file 2 is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual_2));
    }
}
