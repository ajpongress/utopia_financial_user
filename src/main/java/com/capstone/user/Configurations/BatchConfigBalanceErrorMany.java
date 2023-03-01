package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Controllers.UserTransactionController;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.PathHandlers.ReportsPathHandler;
import com.capstone.user.Processors.BalanceErrorManyProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Configuration
@Slf4j
public class BatchConfigBalanceErrorMany {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_Transaction_User")
    private SynchronizedItemStreamReader<UserTransactionModel> synchronizedItemStreamReader;

    @Autowired
    private BalanceErrorManyProcessor balanceErrorManyProcessor;

    @Autowired
    @Qualifier("writer_Transaction_User")
    private ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter;

    @Autowired
    @Qualifier("taskExecutor_User")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private UserTransactionClassifier userTransactionClassifier;

    @Autowired
    private ReportsPathHandler reportsPathHandler;



    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - insufficient balance (more than once)
    @Bean
    public Step step_exportAllBalanceErrors() {

        return new StepBuilder("balanceErrorManyStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(balanceErrorManyProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

                        userTransactionClassifier.closeAllwriters();

                        // Get total users and users with multiple bal error from Processor returnCounters method
                        long totalUsers = balanceErrorManyProcessor.returnCounters().get(0);
                        float totalUsersFloat = (float) totalUsers;
                        long usersWithMultInsufficientBal = balanceErrorManyProcessor.returnCounters().get(1);
                        float usersWithMultInsufficientBalFloat = (float) usersWithMultInsufficientBal;
                        float percentageOfUsers = usersWithMultInsufficientBalFloat / totalUsersFloat;

                        // Create reports file using reports file path from Controller API call
//                        String filePath = UserTransactionController.getReportsPath();
                        String filePath = reportsPathHandler.getReportsPath();
                        File multInsufficientBalanceReport = new File(filePath + "/reports");

                        // Write relevant data to reports file
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(multInsufficientBalanceReport));
                            writer.write("Total users = " + totalUsers);
                            writer.newLine();
                            writer.write("# of users with multiple insufficient balance errors = " + usersWithMultInsufficientBal);
                            writer.newLine();
                            writer.write("% of total users with insufficient balance more than once = " + percentageOfUsers);
                            writer.newLine();
                            writer.write("Total number of transactions with \"Insufficient Balance\" error = " + stepExecution.getWriteCount());
                            writer.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        // Print same data to console
                        log.info("------------------------------------------------------------------");
                        log.info("Total users = " + totalUsers);
                        log.info("# of users with multiple insufficient balance errors = " + usersWithMultInsufficientBal);
                        log.info("% of total users with insufficient balance more than once = " + percentageOfUsers);
                        log.info("Total number of transactions with \"Insufficient Balance\" error = " + stepExecution.getWriteCount());
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        balanceErrorManyProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - insufficient balance (more than once)
    @Bean
    public Job job_exportAllBalanceErrors() {

        return new JobBuilder("balanceErrorManyJob", jobRepository)
                .start(step_exportAllBalanceErrors())
                .build();
    }
}
