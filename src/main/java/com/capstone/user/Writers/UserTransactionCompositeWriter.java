package com.capstone.user.Writers;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserTransactionCompositeWriter {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    UserTransactionClassifier classifierUser;



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    @Bean("writer_Transaction_User")
    public ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter() {

        ClassifierCompositeItemWriter<UserTransactionModel> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(classifierUser);

        return writer;
    }
}
