package com.capstone.user.Processors;

import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@StepScope
@Component
@Slf4j
public class BalanceErrorOnceProcessor implements ItemProcessor<UserTransactionModel, UserTransactionModel> {

    // This processor checks for the % of users who have had transactions that
    // triggered an "insufficient balance" error at least once

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Pair userID (as primary key) to Boolean
    // If user had insufficient balance error, Boolean = true
    // Used for parsing through multiple transactions by same user
    private HashMap<Long, Boolean> userErrorTracker = new HashMap<>();

    // Counter for unique total users
    private static long totalUserCounter = 0L;

    // Counter for unique users with at least one insufficient balance error
    private static long userErrorCounter = 0L;

    // General ID counter
    private static long transactionIdCounter = 0L;

    // Store totalUser and userError counters in list to export to Step listener
    private List<Long> counters = new ArrayList<>();

    public void clearAllTrackersAndCounters() {
        userErrorTracker.clear();
        totalUserCounter = 0L;
        userErrorCounter = 0L;
        transactionIdCounter = 0L;
        counters.clear();
    }

    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public UserTransactionModel process(UserTransactionModel transaction) {

        synchronized(this) {

            // User hasn't been seen yet (hashmap doesn't contain userID)
            if (!userErrorTracker.containsKey(transaction.getUserID())) {

                totalUserCounter++;
                // Add user to hashmap with "false" for insufficient balance check
                userErrorTracker.put(transaction.getUserID(), false);

                // Check if user transaction has insufficient balance error
                if (transaction.getTransactionErrorCheck().contains("Insufficient Balance")) {

                    userErrorCounter++;
                    // Change user Boolean to "true" for insufficient balance check
                    userErrorTracker.put(transaction.getUserID(), true);

                    // Strip negative sign from MerchantID
                    long temp_MerchantID = Math.abs(transaction.getMerchantID());
                    transaction.setMerchantID(temp_MerchantID);

                    // Strip fractional part of TransactionZip if greater than 5 characters
                    if (transaction.getTransactionZip().length() > 5) {
                        String[] temp_TransactionZip = transaction.getTransactionZip().split("\\.", 0);
                        transaction.setTransactionZip(temp_TransactionZip[0]);
                    }

                    transactionIdCounter++;
                    transaction.setId(transactionIdCounter);
                    log.info(transaction.toString());
                    return transaction;
                }

            }

            // User has been seen and is already in the hashmap

            // Check if Boolean value in hashmap is "false"
            if (userErrorTracker.get(transaction.getUserID()).equals(false)) {

                // Check if user transaction has insufficient balance error
                if (transaction.getTransactionErrorCheck().contains("Insufficient Balance")) {

                    userErrorCounter++;
                    // Change user Boolean to "true" for insufficient balance check
                    userErrorTracker.put(transaction.getUserID(), true);

                    // Strip negative sign from MerchantID
                    long temp_MerchantID = Math.abs(transaction.getMerchantID());
                    transaction.setMerchantID(temp_MerchantID);

                    // Strip fractional part of TransactionZip if greater than 5 characters
                    if (transaction.getTransactionZip().length() > 5) {
                        String[] temp_TransactionZip = transaction.getTransactionZip().split("\\.", 0);
                        transaction.setTransactionZip(temp_TransactionZip[0]);
                    }

                    transactionIdCounter++;
                    transaction.setId(transactionIdCounter);
                    log.info(transaction.toString());
                    return transaction;
                }

            }

            // Neither if condition was satisfied (user was seen but already
            // had the insufficient balance error recorded)
            return null;
        }
    }

    public List<Long> returnCounters() {

        counters.add(totalUserCounter); // Index 0
        counters.add(userErrorCounter); // Index 1

        return counters;
    }

}
