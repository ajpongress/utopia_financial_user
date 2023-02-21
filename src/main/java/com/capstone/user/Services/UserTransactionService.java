package com.capstone.user.Services;

import com.capstone.user.Configurations.*;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.NoSuchElementException;

@Service
public class UserTransactionService {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    BatchConfigAllUsers batchConfigAllUsers;

    @Autowired
    BatchConfigSingleUser batchConfigSingleUser;

    @Autowired
    BatchConfigBalanceErrorOnce batchConfigBalanceErrorOnce;

    @Autowired
    BatchConfigBalanceErrorMany batchConfigBalanceErrorMany;

    @Autowired
    BatchConfigDerivedPayments batchConfigDerivedPayments;

    // Job Parameters - All Users
    private JobParameters buildJobParameters_UserDefault(String pathInput, String pathOutput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addLong("time.Started", System.currentTimeMillis())
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .toJobParameters();
    }

    // ----------------------------------------------------------------------------------------------------------

    // Job Parameters - Single User
    private JobParameters buildJobParameters_SingleUser(long userID, String pathInput, String pathOutput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addLong("time.Started", System.currentTimeMillis())
                .addLong("userID_param", userID)
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .toJobParameters();
    }

    // ----------------------------------------------------------------------------------------------------------

    // Job Parameters - Check users for at least one insufficient balance error
    private JobParameters buildJobParameters_BalanceError(String pathInput, String pathOutput, String pathReports) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addLong("time.Started", System.currentTimeMillis())
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .addString("reportPath_param", pathReports)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // all users
    public ResponseEntity<String> allUsers(String pathInput, String pathOutput) {

        try {
            JobParameters jobParameters = buildJobParameters_UserDefault(pathInput, pathOutput);
            jobLauncher.run(batchConfigAllUsers.job_allUsers(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }

    // ----------------------------------------------------------------------------------------------------------

    // specific user
    public ResponseEntity<String> singleUser(long userID, String pathInput, String pathOutput) {

        try {
            if (userID < 0) {
                return new ResponseEntity<>("User ID format invalid", HttpStatus.BAD_REQUEST);
            }
            else {
                JobParameters jobParameters = buildJobParameters_SingleUser(userID, pathInput, pathOutput);
                jobLauncher.run(batchConfigSingleUser.job_singleUser(), jobParameters);
            }

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NumberFormatException e) {
            return new ResponseEntity<>("User ID format invalid", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }

    // ----------------------------------------------------------------------------------------------------------

    // insufficient balance error (at least once)
    public ResponseEntity<String> balanceErrorOnce(String pathInput, String pathOutput, String pathReports) {

        try {
            JobParameters jobParameters = buildJobParameters_BalanceError(pathInput, pathOutput, pathReports);
            jobLauncher.run(batchConfigBalanceErrorOnce.job_exportFirstBalanceError(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }

    // ----------------------------------------------------------------------------------------------------------

    // insufficient balance error (more than once)
    public ResponseEntity<String> balanceErrorMany(String pathInput, String pathOutput, String pathReports) {

        try {
            JobParameters jobParameters = buildJobParameters_BalanceError(pathInput, pathOutput, pathReports);
            jobLauncher.run(batchConfigBalanceErrorMany.job_exportAllBalanceErrors(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }

    // ----------------------------------------------------------------------------------------------------------

    // derived user payments
    public ResponseEntity<String> derivedPayments(long userID, String pathInput, String pathOutput) {

        try {
            JobParameters jobParameters = buildJobParameters_SingleUser(userID, pathInput, pathOutput);
            jobLauncher.run(batchConfigDerivedPayments.job_exportUserDerivedPayments(), jobParameters);

        } catch (BeanCreationException e) {
            return new ResponseEntity<>("Bean creation had an error. Job halted.", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }
}
