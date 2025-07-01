package com.deloitte.client.apacheigniteclient.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Employee {
    @QuerySqlField(index = true)
    private String id;

    @QuerySqlField
    private String name;

    @QuerySqlField
    private String country;

    public Employee() {
    }

    public Employee(String id, String name, String country) {
        this.id = id;
        this.name = name;
        this.country = country;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setNamr(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id='" + id + '\'' +
                ", firstName='" + name + '\'' +
                ", lastName='" + country + '\'' +
                '}';
    }
}
