package com.springml.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/**
 * Model class for Campaign name and category
 */
@DefaultCoder(AvroCoder.class)
public class Campaign {
    public String name;
    public String category;

    public Campaign() {
    }

    public Campaign(String name, String category) {
        this.name = name;
        this.category = category;
    }

    @Override
    public String toString() {
        return "Campaign{" +
                "name='" + name + '\'' +
                ", category='" + category + '\'' +
                '}';
    }
}
