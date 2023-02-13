package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Controllers.UserTransactionController;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.BalanceErrorOnceProcessor;
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
public class BatchConfigBalanceErrorOnce {

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
    private BalanceErrorOnceProcessor balanceErrorOnceProcessor;

    @Autowired
    @Qualifier("writer_Transaction_User")
    private ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter;

    @Autowired
    @Qualifier("taskExecutor_User")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private UserTransactionClassifier userTransactionClassifier;

    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - insufficient balance (at least once)
    @Bean
    public Step step_exportFirstBalanceError() {

        return new StepBuilder("balanceErrorOnceStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(balanceErrorOnceProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

                        userTransactionClassifier.closeAllwriters();

                        // Get total users and users with bal error from Processor returnCounters method
                        long totalUsers = balanceErrorOnceProcessor.returnCounters().get(0);
                        float totalUsersFloat = (float) totalUsers;
                        long usersWithInsufficientBal = balanceErrorOnceProcessor.returnCounters().get(1);
                        float usersWithInsufficientBalFloat = (float) usersWithInsufficientBal;
                        float percentageOfUsers = usersWithInsufficientBalFloat / totalUsersFloat;

                        // Create reports file using reports file path from Controller API call
                        String filePath = UserTransactionController.getReportsPath();
                        File insufficientBalanceReport = new File(filePath);

                        // Write relevant data to reports file
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(insufficientBalanceReport));
                            writer.write("Total users = " + totalUsers);
                            writer.newLine();
                            writer.write("# of users with insufficient balance at least once = " + usersWithInsufficientBal);
                            writer.newLine();
                            writer.write("% of total users with insufficient balance at least once = " + percentageOfUsers);
                            writer.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        // Print same data to console
                        log.info("------------------------------------------------------------------");
                        log.info("Total users = " + totalUsers);
                        log.info("# of users with insufficient balance at least once = " + usersWithInsufficientBal);
                        log.info("% of total users with insufficient balance at least once = " + percentageOfUsers);
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        balanceErrorOnceProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - insufficient balance (at least once)
    @Bean
    public Job job_exportFirstBalanceError() {

        return new JobBuilder("balanceErrorOnceJob", jobRepository)
                .start(step_exportFirstBalanceError())
                .build();
    }
}
