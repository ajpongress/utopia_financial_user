package com.capstone.user.Processors;

import com.capstone.user.Models.UserTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.*;

@StepScope
@Component
@Slf4j
public class BalanceErrorManyProcessor implements ItemProcessor<UserTransactionModel, UserTransactionModel> {

    // This processor checks for the % of users who have had transactions that
    // triggered an "insufficient balance" error more than once

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Pair userID (as primary key) to Long (0, 1, or 2+)
    // If user had insufficient balance error once, Long = 1
    // If user had insufficient balance error more than once, Long = 2
    // Used for parsing through multiple transactions by same user
    private HashMap<Long, Long> userErrorTracker = new HashMap<>();

    // Counter for unique total users
    private static long totalUserCounter = 0L;

    // Counter for unique users with more than one insufficient balance error
    private static long userMultErrorCounter = 0L;

    // General ID counter
    private static long transactionIdCounter = 0L;

    // Store totalUser and userError counters in list to export to Step listener
    private List<Long> counters = new ArrayList<>();

    public void clearAllTrackersAndCounters() {
        userErrorTracker.clear();
        totalUserCounter = 0L;
        userMultErrorCounter = 0L;
        transactionIdCounter = 0L;
        counters.clear();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public UserTransactionModel process(UserTransactionModel transaction) {

        synchronized(this) {

            // User hasn't been seen yet
            if (!userErrorTracker.containsKey(transaction.getUserID())) {

                totalUserCounter++;
                userErrorTracker.put(transaction.getUserID(), 0L); // Add user to hashmap with 0 for multiple insufficient balance check

                // Check if first user transaction has insufficient balance error
                if (transaction.getTransactionErrorCheck().contains("Insufficient Balance")) {
                    userErrorTracker.replace(transaction.getUserID(), 0L, 1L); // Increment Long to 1 in Hashmap for user
                }

                return null; // User doesn't have multiple balance errors yet
            }

            // User has been seen and userID is recorded in Hashset
            else {

                // Check if user transaction has insufficient balance error on next transaction
                if (transaction.getTransactionErrorCheck().contains("Insufficient Balance")) {

                    // Check if user hasn't had a balance error recorded yet
                    if (userErrorTracker.get(transaction.getUserID()).equals(0L)) {
                        userErrorTracker.replace(transaction.getUserID(), 0L, 1L); // Increment Integer to 1 in Hashmap for user
                    }

                    // Check if user already has one recorded balance error (userMultErrorCounter is incremented here only)
                    else if (userErrorTracker.get(transaction.getUserID()).equals(1L)) {

                        userErrorTracker.replace(transaction.getUserID(), 1L, 2L); // Increment Integer to 2 in Hashmap for user
                        userMultErrorCounter++;
                        // Return user transaction to write to user's transaction list
                        transactionIdCounter++;
                        transaction.setId(transactionIdCounter);
                        log.info(transaction.toString());
                        return transaction;
                    }

                    // User has more than 1 balance error recorded (value will be 2L)
                    // (return transaction to write to file but don't increment userMultErrorCounter)
                    // Long in Hashmap doesn't need to be incremented anymore. All future transactions
                    // by this user with a balance error will be caught here and sent to writer
                    else {

                        // Return user transaction to write to user's transaction list
                        transactionIdCounter++;
                        transaction.setId(transactionIdCounter);
                        log.info(transaction.toString());
                        return transaction;
                    }
                }

                return null;
            }
        }
    }

    public List<Long> returnCounters() {

        counters.add(totalUserCounter); // Index 0
        counters.add(userMultErrorCounter); // Index 1

        return counters;
    }
}
