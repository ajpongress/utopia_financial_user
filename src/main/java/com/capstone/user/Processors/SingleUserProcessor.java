package com.capstone.user.Processors;

import com.capstone.user.Models.UserTransactionModel;
import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;

@StepScope
@Component
@Slf4j
public class SingleUserProcessor implements ItemProcessor<UserTransactionModel, UserTransactionModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // UserID for specific user transaction export
    @Value("#{jobParameters['userID_param']}")
    private long userID_from_URI;

    // Tracks userID as primary Key and attaches user first/last name to the key
    // ArrayList -> Index 0 (firstName) / Index 1 (lastName)
    private final HashMap<Long, ArrayList<String>> userIdTracker = new HashMap<>();

    private final Faker faker = new Faker();

    private static long transactionIdCounter = 0;

    public void clearAllTrackersAndCounters() {
        userIdTracker.clear();
        transactionIdCounter = 0;
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public UserTransactionModel process(UserTransactionModel transactionUser) {

        synchronized (this) {

            // Filter transactions by requested UserID from REST call
            if (transactionUser.getUserID() == (userID_from_URI)) {

                // -------------------- NEW USER -------------------------
                // If userID tracker doesn't contain user, generate first/last name for new user
                // (avoids setting multiple first/last names per user)
                if (!userIdTracker.containsKey(transactionUser.getUserID())) {

                    // Generate first/last name & email
                    transactionUser.setUserFirstName(faker.name().firstName());
                    transactionUser.setUserLastName(faker.name().lastName());
                    transactionUser.generateUserEmail();

                    // Add first/last name to userIdTracker
                    userIdTracker.put(transactionUser.getUserID(), new ArrayList<>());
                    userIdTracker.get(transactionUser.getUserID()).add(transactionUser.getUserFirstName());
                    userIdTracker.get(transactionUser.getUserID()).add(transactionUser.getUserLastName());
                }

                // -------------------- EXISTING USER -------------------------
                // Set user first/last name & email from existing Key in userIdTracker
                if (userIdTracker.containsKey(transactionUser.getUserID())) {

                    // Get index 0 (firstName) and 1 (lastName) from ArrayList and set to transactionUser
                    transactionUser.setUserFirstName(userIdTracker.get(transactionUser.getUserID()).get(0));
                    transactionUser.setUserLastName(userIdTracker.get(transactionUser.getUserID()).get(1));
                    transactionUser.generateUserEmail();
                }

                // Strip negative sign from MerchantID
                long temp_MerchantID = Math.abs(transactionUser.getMerchantID());
                transactionUser.setMerchantID(temp_MerchantID);

                // Strip fractional part of TransactionZip if greater than 5 characters
                if (transactionUser.getTransactionZip().length() > 5) {
                    String[] temp_TransactionZip = transactionUser.getTransactionZip().split("\\.", 0);
                    transactionUser.setTransactionZip(temp_TransactionZip[0]);
                }

                // Increment transactionUser ID, print processed transactionUser and return
                transactionIdCounter++;
                transactionUser.setId(transactionIdCounter);
                log.info(transactionUser.toString());
                return transactionUser;
            }

            // Discard all other transactions that aren't the requested userID
            else return null;

        }
    }
}
