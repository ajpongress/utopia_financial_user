package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Controllers.UserTransactionController;
import com.capstone.user.Listeners.CustomChunkListener;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.DerivedPaymentsProcessor;
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
public class BatchConfigDerivedPayments {

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
    private DerivedPaymentsProcessor derivedPaymentsProcessor;

    @Autowired
    @Qualifier("writer_Transaction_User")
    private ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter;

//    @Autowired
//    @Qualifier("taskExecutor_User")
//    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private UserTransactionClassifier userTransactionClassifier;



    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - derived user payments
    @Bean
    public Step step_exportUserDerivedPayments() {

        return new StepBuilder("exportUserDerivedPaymentsStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(derivedPaymentsProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new CustomChunkListener())
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

                        userTransactionClassifier.closeAllwriters();

                        // Create reports file using reports file path from Controller API call
                        String filePath = UserTransactionController.getReportsPath();
                        File derivedPaymentsReport = new File(filePath + "/user_" + derivedPaymentsProcessor.getUserID_from_URI() + "_reports");

                        long totalUserTransactions = derivedPaymentsProcessor.getTotalTransactionCounter();
                        long totalInsufficientBal = derivedPaymentsProcessor.getInsufficientBalCounter();
                        long totalDerivedPayments = derivedPaymentsProcessor.getTransactionIdCounter();

                        // Write relevant data to reports file
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(derivedPaymentsReport));

                            writer.write("User " + derivedPaymentsProcessor.getUserID_from_URI() + " reports:");
                            writer.newLine();
                            writer.newLine();
                            writer.write("Total transactions - " + totalUserTransactions);
                            writer.newLine();
                            writer.write("Total \"Insufficient Balance\" errors - " + totalInsufficientBal);
                            writer.newLine();
                            writer.newLine();
                            writer.write("(Derived payments are inferred from the first successful transaction made immediately after a transaction with an \"Insufficient Balance\" error");
                            writer.newLine();
                            writer.write("Derived payments - " + totalDerivedPayments);

                            writer.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        derivedPaymentsProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                //.taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - derived user payments
    @Bean
    public Job job_exportUserDerivedPayments() {

        return new JobBuilder("exportUserDerivedPaymentsJob", jobRepository)
                .start(step_exportUserDerivedPayments())
                .build();
    }
}
