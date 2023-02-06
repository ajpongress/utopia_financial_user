package com.capstone.user;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Configurations.BatchConfigSingleUser;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.SingleUserProcessor;
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
//                          Test Single User Operations
// ********************************************************************************

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigSingleUser.class,
        UserTransactionClassifier.class,
        UserTransactionModel.class,
        UserTransactionReaderCSV.class,
        SingleUserProcessor.class,
        UserTransactionCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class SpringBatchIntegrationTests_SingleUserTransaction {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Set userID to test for single user operations & export
    private long userID = 2;
    private String INPUT = "src/test/resources/input/test_input.csv";
    private String EXPECTED_OUTPUT = "src/test/resources/output/test_expected_output_UserTransaction.xml";
    private String ACTUAL_OUTPUT = "src/test/resources/output/user_" + userID;

//    @BeforeEach
//    public void setup(@Autowired Job job_singleUser) {
//        jobLauncherTestUtils.setJob(job_singleUser);
//    }

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_SingleUser() {

        return new JobParametersBuilder()
                .addLong("userID_param", userID)
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }


    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_SingleUser() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_SingleUser());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileExpected = new File(EXPECTED_OUTPUT);
        File testOutputFileActual = new File(ACTUAL_OUTPUT + "/user_" + userID + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("singleUserJob", actualJobInstance.getJobName());
        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());
        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected));
        // Verify output (expected) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));
        // Verify output (actual) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual));
    }
}

