package com.gs.customerPoliciesNoDIH.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class

MsSqlServerProps {

    @Value("${MsSqlServer.host}")
    private String host;
    @Value("${MsSqlServer.port}")
    private String port;
    @Value("${MsSqlServer.username}")
    private String name;
    @Value("${MsSqlServer.password}")
    private String password;
    @Value("${MsSqlServer.database}")
    private String database;


    public void setHost(String host) {
        this.host = host;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public String getUrl() {
        return String.format("jdbc:sqlserver://;serverName=%s;port=%s;databaseName=%s;trustServerCertificate=true",host, port, database);
    }

    public String getName() {
        return name;
    }

    public void setDatabase(String database) {
        this.database = database;
    }


    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public String toString() {
        return "MsSqlServerProps{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", name='" + name + '\'' +
                ", password='" + password + '\'' +
                ", database='" + database + '\'' +
                '}';
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}