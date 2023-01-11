package com.gs.customerPoliciesNoDIH;

import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/")
@Validated
@CrossOrigin
public class CustomerPoliciesController {
    private final Validator validator;
    private final CustomerPoliciesService customerPolicies;

    public CustomerPoliciesController(CustomerPoliciesService customerPolicies, Validator validator) {
        this. customerPolicies = customerPolicies;
        this.validator = validator;
    }

    @GetMapping("legacyCustomerPolicies")
    public List<Map> customerPolicies(@RequestParam Optional<String> state) throws SQLException {
        return
                customerPolicies.customerPolicies(state);
    }

    @GetMapping("legacyCustomerPolicies2")
    public List<Map> customerPolicies2(@RequestParam Optional<String> state) throws SQLException {
        return
                customerPolicies.customerPolicies2(state);
    }


}
