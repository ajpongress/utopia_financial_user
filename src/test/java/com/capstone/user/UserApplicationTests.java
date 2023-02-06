package com.capstone.user;

import com.capstone.user.Models.UserTransactionModel;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class UserApplicationTests {

    @Test
    void contextLoads() {
    }

    // Model testing
    @Test
    public void creates_transaction_id_1_userid_99_firstname_Bob_lastname_Smith_email_bobdotsmithatsmoothceeplusplus_cardid_88_year_2023_month_3_day_7_time_1130_amount_101_11_type_swipe_transaction_merchantid_777777777_city_Chicago_state_IL_zip_60602_merchantcode_5555_error_no_fraud_no () throws ClassNotFoundException {

        UserTransactionModel transactionUser = new UserTransactionModel();
        transactionUser.setId(1);
        transactionUser.setUserID(99);
        transactionUser.setUserFirstName("Bob");
        transactionUser.setUserLastName("Smith");
        transactionUser.generateUserEmail();
        transactionUser.setCardID(88);
        transactionUser.setTransactionYear("2023");
        transactionUser.setTransactionMonth("3");
        transactionUser.setTransactionDay("7");
        transactionUser.setTransactionTime("11:30");
        transactionUser.setTransactionAmount("$101.11");
        transactionUser.setTransactionType("Swipe UserTransactionModel");
        transactionUser.setMerchantID(777777777);
        transactionUser.setTransactionCity("Chicago");
        transactionUser.setTransactionState("IL");
        transactionUser.setTransactionZip("60602");
        transactionUser.setMerchantCatCode(5555);
        transactionUser.setTransactionErrorCheck("Yes");
        transactionUser.setTransactionFraudCheck("No");

        assertEquals(1, transactionUser.getId());
        assertEquals(99, transactionUser.getUserID());
        assertEquals("Bob", transactionUser.getUserFirstName());
        assertEquals("Smith", transactionUser.getUserLastName());
        assertEquals("bob.smith@smoothceeplusplus.com", transactionUser.getUserEmail());
        assertEquals(88, transactionUser.getCardID());
        assertEquals("2023", transactionUser.getTransactionYear());
        assertEquals("3", transactionUser.getTransactionMonth());
        assertEquals("7", transactionUser.getTransactionDay());
        assertEquals("11:30", transactionUser.getTransactionTime());
        assertEquals("$101.11", transactionUser.getTransactionAmount());
        assertEquals("Swipe UserTransactionModel", transactionUser.getTransactionType());
        assertEquals(777777777, transactionUser.getMerchantID());
        assertEquals("Chicago", transactionUser.getTransactionCity());
        assertEquals("IL", transactionUser.getTransactionState());
        assertEquals("60602", transactionUser.getTransactionZip());
        assertEquals(5555, transactionUser.getMerchantCatCode());
        assertEquals("Yes", transactionUser.getTransactionErrorCheck());
        assertEquals("No", transactionUser.getTransactionFraudCheck());
    }
}
