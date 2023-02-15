package com.capstone.user.Controllers;

import com.capstone.user.Services.UserTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class UserTransactionController {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    private static String reportsPath;

    public static String getReportsPath() {
        return reportsPath;
    }

    @Autowired
    UserTransactionService transactionServiceUser;



    // ----------------------------------------------------------------------------------
    // --                               MAPPINGS                                       --
    // ----------------------------------------------------------------------------------

    // all users
    // https://{server-address}/users?destination={destination} - to export separate files all users provided from the flat file
    @GetMapping("/users")
    public ResponseEntity<String> allUsersAPI(@RequestParam String source, @RequestParam String destination) {

        return transactionServiceUser.allUsers(source, destination);
    }

    // specific userID
    // https://{server-address}/users/{user-id}?destination={destination} - to export file for specified user provided from the flat file
    @GetMapping("/users/{userID}")
    public ResponseEntity<String> oneUserAPI(@PathVariable long userID, @RequestParam String source, @RequestParam String destination) {

        // Ensure userID is positive
        //userID = Math.abs(userID); // Comment for DEMO!!!!!
        return transactionServiceUser.singleUser(userID, source, destination);
    }

    // check for insufficient balance at least once per user
    @GetMapping("/users/balance_error_once")
    public ResponseEntity<String> balanceErrorOnceAPI(@RequestParam String source, @RequestParam String destination, @RequestParam String reports_destination) {

        reportsPath = reports_destination;
        return transactionServiceUser.balanceErrorOnce(source, destination, reports_destination);
    }

    // check for insufficient balance more than once per user
    @GetMapping("/users/balance_error_many")
    public ResponseEntity<String> balanceErrorManyAPI(@RequestParam String source, @RequestParam String destination, @RequestParam String reports_destination) {

        reportsPath = reports_destination;
        return transactionServiceUser.balanceErrorMany(source, destination, reports_destination);
    }


}
