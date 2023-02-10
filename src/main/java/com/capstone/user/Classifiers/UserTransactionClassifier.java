package com.capstone.user.Classifiers;

import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.Classifier;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@StepScope
@Component
@Slf4j
public class UserTransactionClassifier implements Classifier<UserTransactionModel, ItemWriter<? super UserTransactionModel>> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Destination path for export file
    @Value("#{jobParameters['outputPath_param']}")
    private String outputPath;

    //private static final Map<String, ItemWriter<? super UserTransactionModel>> writerMap = new HashMap<>();

    // Map for mapping each userID to its own dedicated ItemWriter (for performance)
    private final Map<String, ItemWriter<? super UserTransactionModel>> writerMap;

    // Public constructor
    public UserTransactionClassifier() {
        this.writerMap = new HashMap<>();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // Classify method (contains XML writer and synchronized item stream writer)
    @Override
    public ItemWriter<? super UserTransactionModel> classify(UserTransactionModel transactionUser) {

        // Set filename to specific userID from the UserTransactionModel model
        String fileName = transactionUser.getFileName();



        // Make entire process thead-safe
        synchronized (this) {

            // If userID has already been accessed, use the same ItemWriter
            if (writerMap.containsKey(fileName)) {
                return writerMap.get(fileName);
            }
            // Create new ItemWriter for new UserID
            else {

                // Complete path for file export
                File file = new File(outputPath + "\\" + fileName);
//                // ================= DEBUG ====================
//                log.info("============================================================");
//                log.info("File Path = " + file.getAbsolutePath());
//                log.info("============================================================");

                // XML writer
                XStreamMarshaller marshaller = new XStreamMarshaller();
                marshaller.setAliases(Collections.singletonMap("transactionUser", UserTransactionModel.class));

                StaxEventItemWriter<UserTransactionModel> writerXML = new StaxEventItemWriterBuilder<UserTransactionModel>()
                        .name("userXmlWriter")
                        .resource(new FileSystemResource(file))
                        .marshaller(marshaller)
                        .rootTagName("transactions")
                        .transactional(false) // Keeps XML headers on all output files
                        .build();

                // Make XML writer thread-safe
                SynchronizedItemStreamWriter<UserTransactionModel> synchronizedItemStreamWriter =
                        new SynchronizedItemStreamWriterBuilder<UserTransactionModel>()
                                .delegate(writerXML)
                                .build();

                writerXML.open(new ExecutionContext());
                writerMap.put(fileName, synchronizedItemStreamWriter); // Pair UserID to unique ItemWriter
                return synchronizedItemStreamWriter;
            }
        }
    }

    public void closeAllwriters() {

        for (String key : writerMap.keySet()) {

            SynchronizedItemStreamWriter<UserTransactionModel> writer = (SynchronizedItemStreamWriter<UserTransactionModel>) writerMap.get(key);
            writer.close();
        }
        writerMap.clear();
    }
}
