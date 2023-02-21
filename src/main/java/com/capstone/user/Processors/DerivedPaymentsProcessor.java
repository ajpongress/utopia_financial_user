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
public class DerivedPaymentsProcessor implements ItemProcessor<UserTransactionModel, UserTransactionModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // UserID for specific user transaction export
    @Value("#{jobParameters['userID_param']}")
    private long userID_from_URI;

    public long getUserID_from_URI() {
        return userID_from_URI;
    }

    // Tracks userID as primary Key and attaches user first/last name to the key
    // ArrayList -> Index 0 (firstName) / Index 1 (lastName)
    private final HashMap<Long, ArrayList<String>> userIdTracker = new HashMap<>();

    // Keep count of transactions made immediately after an "Insufficient Balance" transaction
    private static long transactionIdCounter = 0L;

    private static long insufficientBalCounter = 0L;

    private static long totalTransactionCounter = 0L;

    public long getTransactionIdCounter() {
        return transactionIdCounter;
    }

    public long getInsufficientBalCounter() {
        return insufficientBalCounter;
    }

    public long getTotalTransactionCounter() {
        return totalTransactionCounter;
    }

    boolean balanceErrorTriggered = false;

    private final Faker faker = new Faker();

    public void clearAllTrackersAndCounters() {
        userIdTracker.clear();
        transactionIdCounter = 0L;
        insufficientBalCounter = 0L;
        totalTransactionCounter = 0L;
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public UserTransactionModel process(UserTransactionModel transactionUser) {

        // Filter transactions by requested UserID from REST call
        if (transactionUser.getUserID() == (userID_from_URI)) {

            // Increment total transaction counter for specified user
            totalTransactionCounter++;

            // Model setup

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
            else { // Set user first/last name & email from existing Key in userIdTracker

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

            // END Model setup

            // Return the first transaction after an Insufficient Balance error that doesn't
            // contain an error
            // (implies user made payment to account to get out of the negative)
            // Return these types of transactions (the # of them indicate how many payments
            // the user made at a minimum)

            // If the next consecutive transaction has balance error, update counter
            // (must be checked before the flag is set)
            if (transactionUser.getTransactionErrorCheck().contains("Insufficient Balance")
                    && balanceErrorTriggered == true) {
                insufficientBalCounter++;
            }

            // Find a fresh balance error and set the boolean flag
            if (transactionUser.getTransactionErrorCheck().contains("Insufficient Balance")
                    && balanceErrorTriggered == false) {

                insufficientBalCounter++;
                balanceErrorTriggered = true;
            }

            // Find the next transaction without a balance error and write it to XML
            if (!transactionUser.getTransactionErrorCheck().contains("Insufficient Balance")
                    && balanceErrorTriggered == true) {

                // Reset flag
                balanceErrorTriggered = false;
                // Set transactionID and write to XML string
                transactionIdCounter++;
                transactionUser.setId(transactionIdCounter);
                return transactionUser;
            }

        }

        return null;

    }

}
