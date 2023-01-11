package com.gs.customerPoliciesNoDIH;

import com.gs.customerPoliciesNoDIH.config.MsSqlServerProps;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.bson.Document;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.*;

@Service
public class CustomerPoliciesService {
    private static final Logger logger = LoggerFactory.getLogger(CustomerPoliciesService.class);

    static {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Autowired
    MsSqlServerProps msSqlServerProps;

    @Value("${mongo.url}")
    String mongoUrl;

    @Value("${mongo.database}")
    String mongoDB;

    @PostConstruct
    public void CustomerPoliciesServicePostConstruct(){
        logger.info(mongoUrl);
        logger.info(msSqlServerProps.getUrl());
    }

    public List<Map> customerPolicies(Optional<String> state) throws SQLException {
        List list = new ArrayList();
        try (Connection con = DriverManager.getConnection(msSqlServerProps.getUrl(),
                msSqlServerProps.getName(), msSqlServerProps.getPassword())) {
            MongoCursor<Document> cursor = fetchCustomers(state);
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                logger.debug("Fetching customer email: " + doc.get("email"));
                PreparedStatement stmt = con.prepareStatement("select p.policyId, p.policyStartDate, p.policyEndDate," +
                        " pr.productCategory, pr.productName " +
                        "from policy p left join product pr on p.productId=pr.productId where p.customerId = ?");
                stmt.setInt(1,(Integer) doc.get("customerId"));
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Map record = new HashMap();
                    record.put("policyId", rs.getInt("policyId"));
                    record.put("policyStartDate", rs.getDate("policyStartDate"));
                    record.put("policyEndDate", rs.getDate("policyEndDate"));
                    record.put("productCategory", rs.getString("productCategory"));
                    record.put("productName", rs.getString("productName"));
                    record.put("firstName", doc.get("firstName"));
                    record.put("lastName", doc.get("lastName"));
                    record.put("email", doc.get("email"));
                    list.add(record);
                }
            }
        }
        logger.debug("Finished iterating customers");
        return list;
    }



    public List<Map> customerPolicies2(Optional<String> state) throws SQLException {
        List<Map> customersList = new ArrayList();
        List list = new ArrayList();
        Map<Integer, Map> customerPolicies = new HashMap<>();
        try (Connection con = DriverManager.getConnection(msSqlServerProps.getUrl(),
                msSqlServerProps.getName(), msSqlServerProps.getPassword())) {

            MongoCursor<Document> cursor = fetchCustomers(state);
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Map record = new HashMap();
                record.put("customerId", doc.get("customerId"));
                record.put("firstName", doc.get("firstName"));
                record.put("lastName", doc.get("lastName"));
                record.put("email", doc.get("email"));
                customersList.add(record);
            }
            PreparedStatement stmt = con.prepareStatement("select p.customerId, p.policyId, p.policyStartDate, p.policyEndDate," +
                        " pr.productCategory, pr.productName " +
                        "from policy p left join product pr on p.productId=pr.productId");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Map record = new HashMap();
                record.put("policyId", rs.getInt("policyId"));
                record.put("policyStartDate", rs.getDate("policyStartDate"));
                record.put("policyEndDate", rs.getDate("policyEndDate"));
                record.put("productCategory", rs.getString("productCategory"));
                record.put("productName", rs.getString("productName"));
                customerPolicies.put(rs.getInt("customerId"), record);
            }
        }
        for (Map record:
             customersList) {
            Map policies = customerPolicies.get(record.get("customerId"));
            if(policies!=null) {
                policies.putAll(record);
                policies.remove("customerId");
                list.add(policies);
            } else {
                logger.debug("Skipped customer: " + record.get("customerId"));
            }
        }
        logger.debug("Finished iterating customers");
        return list;
    }

    private MongoCursor<Document> fetchCustomers(Optional<String> state){
        MongoClientURI uri = new MongoClientURI(mongoUrl);
        MongoClient mongo = new MongoClient(uri);
        MongoDatabase database = mongo.getDatabase(mongoDB);
        MongoCollection<Document> aa = database.getCollection("customer");
        FindIterable<Document> fi;
        if(state.isEmpty()) {

            fi = aa.find().limit(1000000).batchSize(1000);
            logger.info("Iterating all customers ");
        }
        else {
            Bson bson = (Bson) com.mongodb.util.JSON.parse("{'stateAbbreviation': '" + state.get() + "'}");
            fi = aa.find(bson).limit(1000000).batchSize(1000);
            logger.info("Iterating customers from state: " + state.get());
        }

        return fi.cursorType(CursorType.NonTailable).iterator();
    }

}

