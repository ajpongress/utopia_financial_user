package com.capstone.user;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Configurations.BatchConfigDerivedPayments;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.PathHandlers.ReportsPathHandler;
import com.capstone.user.Processors.DerivedPaymentsProcessor;
import com.capstone.user.Readers.UserTransactionReaderCSV;
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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigDerivedPayments.class,
        UserTransactionClassifier.class,
        UserTransactionModel.class,
        UserTransactionReaderCSV.class,
        DerivedPaymentsProcessor.class,
        UserTransactionCompositeWriter.class,
        ReportsPathHandler.class
        //TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_DerivedPayments {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Set userID to test for single user operations & export
    private long userID = 41;

    // Expected dollar amount returned from specific transaction based on given userID
    private String expectedDollarAmount = "$179.81";

    private String INPUT = "src/test/resources/input/test_input_insufficientBalanceMany.csv";

//    private String REPORTS_OUTPUT = "src/test/resources/output/derived_payments/user_" + userID + "_reports";

    private String REPORTS_OUTPUT = "src/test/resources/output/derived_payments";

    private String ACTUAL_OUTPUT = "src/test/resources/output/derived_payments";

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_DerivedPayments() {

        return new JobParametersBuilder()
                .addLong("userID_param", userID)
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .addString("reportsPath_param", REPORTS_OUTPUT)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_DerivedPayments() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_DerivedPayments());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        //File testOutputFileExpected = new File(EXPECTED_OUTPUT);
        File testOutputFileActual = new File(ACTUAL_OUTPUT + "/user_" + userID + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("exportUserDerivedPaymentsJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (actual) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual));

        // Verify dollar amount matches to returned test transaction
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(testOutputFileActual);
        doc.getDocumentElement().normalize();

        NodeList list = doc.getElementsByTagName("transactionUser");

        for (int temp = 0; temp < list.getLength(); temp++) {

            Node node = list.item(temp);

            if (node.getNodeType() == Node.ELEMENT_NODE) {

                Element element = (Element) node;
                String actualDollarAmount = element.getElementsByTagName("transactionAmount").item(0).getTextContent();
                Assertions.assertEquals(expectedDollarAmount, actualDollarAmount);
            }
        }












    }
}
