package com.springml.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/**
 * Model class for LatLon Key
 */
@DefaultCoder(AvroCoder.class)
public class LatLon {
    public double lat;
    public double lon;

    public LatLon() {}
    public LatLon(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "LatLon{" +
                "lat=" + lat +
                ", lon=" + lon +
                '}';
    }
}
