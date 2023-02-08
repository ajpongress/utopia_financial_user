package com.capstone.user.Processors;

import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.HashMap;

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
    private final HashMap<Long, Boolean> userErrorTracker = new HashMap<>();

    // Counter for unique total users
    private static long totalUserCounter = 0;

    // Counter for unique users with at least one insufficient balance error
    private static long userErrorCounter = 0;

    // General ID counter
    private static long transactionIdCounter = 0;

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
                if (transaction.getTransactionErrorCheck().equals("\"Insufficient Balance")) {

                    userErrorCounter++;
                    // Change user Boolean to "true" for insufficient balance check
                    userErrorTracker.put(transaction.getUserID(), true);
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
                if (transaction.getTransactionErrorCheck().equals("\"Insufficient Balance")) {

                    userErrorCounter++;
                    // Change user Boolean to "true" for insufficient balance check
                    userErrorTracker.put(transaction.getUserID(), true);
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
}
