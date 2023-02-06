package com.capstone.user.Readers;

import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserTransactionReaderCSV {

    // FlatFileItemReader
    @StepScope
    @Bean("reader_Transaction_User")
    public SynchronizedItemStreamReader<UserTransactionModel> synchronizedItemStreamReader(
            @Value("#{jobParameters['file.input']}")
            String source_input
    ) throws UnexpectedInputException,
            NonTransientResourceException, ParseException {

        FlatFileItemReader<UserTransactionModel> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(source_input));
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper((line, lineNumber) -> {
            String[] fields = line.split(",");
            UserTransactionModel transactionUser = new UserTransactionModel();

            transactionUser.setUserID(Long.parseLong(fields[0]));
            transactionUser.setCardID(Long.parseLong(fields[1]));
            transactionUser.setTransactionYear(fields[2]);
            transactionUser.setTransactionMonth(fields[3]);
            transactionUser.setTransactionDay(fields[4]);
            transactionUser.setTransactionTime(fields[5]);
            transactionUser.setTransactionAmount(fields[6]);
            transactionUser.setTransactionType(fields[7]);
            transactionUser.setMerchantID(Long.parseLong(fields[8]));
            transactionUser.setTransactionCity(fields[9]);
            transactionUser.setTransactionState(fields[10]);
            transactionUser.setTransactionZip(fields[11]);
            transactionUser.setMerchantCatCode(Long.parseLong(fields[12]));
            transactionUser.setTransactionErrorCheck(fields[13]);
            transactionUser.setTransactionFraudCheck(fields[14]);

            return transactionUser;
        });

        // Make FlatFileItemReader thread-safe
        return new SynchronizedItemStreamReaderBuilder<UserTransactionModel>()
                .delegate(itemReader)
                .build();
    }
}
