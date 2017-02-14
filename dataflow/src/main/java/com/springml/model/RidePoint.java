package com.springml.model;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Model class to convert TableRow
 */
@DefaultCoder(AvroCoder.class)
public class RidePoint {
    public String rideId;
    public int pointIdx = 0;
    public float lat;
    public float lon;
    public long timestamp;
    public float meterReading;
    public float meterIncrement;
    public String status;
    public int passengerCount;
    public String campaign;
    public boolean userLikedAd;

    public RidePoint() {}

    public RidePoint(String key) {
        rideId = key;
    }

    public RidePoint(RidePoint p) {
        rideId = p.rideId;
        pointIdx = p.pointIdx;
        timestamp = p.timestamp;
        lat = p.lat;
        lon = p.lon;
        meterReading = p.meterReading;
        meterIncrement = p.meterIncrement;
        status = p.status;
        passengerCount = p.passengerCount;
        campaign = p.campaign;
        userLikedAd = p.userLikedAd;
    }

    public RidePoint(TableRow r) {
        rideId = r.get("ride_id").toString();

        Object ptIdx = r.get("point_idx");
        if (ptIdx != null) {
            pointIdx = Integer.parseInt(ptIdx.toString());
        }

        lat = Float.parseFloat(r.get("latitude").toString());
        lon = Float.parseFloat(r.get("longitude").toString());
        timestamp =
                Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(r.get("timestamp").toString()))
                        .toEpochMilli();

        Object mtReading = r.get("meter_reading");
        if (mtReading != null) {
            meterReading = Float.parseFloat(mtReading.toString());
        }

        Object mtInc = r.get("meter_increment");
        if (mtInc != null) {
            meterIncrement = Float.parseFloat(mtInc.toString());
        }

        Object rideStatus = r.get("ride_status");
        if (rideStatus != null) {
            status = rideStatus.toString();
        }

        Object psCount = r.get("passenger_count");
        if (psCount != null) {
            passengerCount = Integer.parseInt(psCount.toString());
        }

        Object cam = r.get("campaign");
        if (cam != null) {
            this.campaign = cam.toString();
        }

        Object likedAd = r.get("user_liked_ad");
        if (likedAd != null) {
            userLikedAd = Boolean.parseBoolean(likedAd.toString());
        }
    }

    public TableRow toTableRow() {
        TableRow result = new TableRow();
        result.set("ride_id", rideId);
        result.set("point_idx", pointIdx);
        result.set("latitude", lat);
        result.set("longitude", lon);
        result.set("timestamp", Instant.ofEpochMilli(timestamp).toString());
        result.set("meter_reading", meterReading);
        result.set("meter_increment", meterIncrement);
        result.set("ride_status", status);
        result.set("passenger_count", passengerCount);
        if (campaign != null) {
            result.set("campaign", campaign);
        }
        result.set("user_liked_ad", userLikedAd);
        return result;
    }
}
