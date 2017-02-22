package com.springml;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.*;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.windowing.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.springml.model.Campaign;
import com.springml.model.LatLon;
import com.springml.model.RidePoint;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Dataflow for Taxi Rides with Ad Campaigns
 */
public class TaxiGDF {
    private static final Logger LOG = LoggerFactory.getLogger(TaxiGDF.class);
    private static final String FILTER_CAMPAIGN = "CAMPAIGN_ENFJ_V1";
    private static final String FILTER_CATEGORY = "travel";
    private static final String CLOUDML_SCOPE =
            "https://www.googleapis.com/auth/cloud-platform";

    private static class TransformCampaign extends SimpleFunction<TableRow, KV<String, Campaign>> {

        @Override
        public KV<String, Campaign> apply(TableRow tableRow) {
            String id = tableRow.get("id").toString();

            String name = tableRow.get("name").toString();
            String category = tableRow.get("category").toString();

            LOG.info("Name of the Campaign : " + name);
            LOG.info("Campaign Category : " + category);
            return KV.of(id, new Campaign(name, category));
        }
    }

    private static class TransformRides extends SimpleFunction<KV<LatLon, Long>, TableRow> {

        @Override
        public TableRow apply(KV<LatLon, Long> ridegrp) {
            TableRow result = new TableRow();
            result.set("latitude", ridegrp.getKey().lat);
            result.set("longitude", ridegrp.getKey().lon);
            result.set("ntaxis", ridegrp.getValue());

            return result;
        }
    }

    private static class MarkRides extends SimpleFunction<TableRow, KV<LatLon, TableRow>> {

        @Override
        public KV<LatLon, TableRow> apply(TableRow t) {
            float lat = Float.parseFloat(t.get("latitude").toString());
            float lon = Float.parseFloat(t.get("longitude").toString());
            LatLon key = new LatLon(lat, lon);

            return KV.of(key, t);
        }
    }

    private static class CloudML extends DoFn<TableRow, TableRow> {
        private String predictRestUrl;

        public CloudML(String  predictRestUrl) {
            this.predictRestUrl = predictRestUrl;
        }

        @Override
        public void processElement(ProcessContext c) throws Exception {
            TableRow tableRow = c.element();
            invokeCloudML();

            c.output(tableRow);
        }

        private void invokeCloudML() {
            try {
                GoogleCredential credential = GoogleCredential.getApplicationDefault()
                        .createScoped(Collections.singleton(CLOUDML_SCOPE));
                HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                HttpRequestFactory requestFactory = httpTransport.createRequestFactory(
                        credential);
                GenericUrl url = new GenericUrl(predictRestUrl);

                JacksonFactory jacksonFactory = new JacksonFactory();
                JsonHttpContent jsonHttpContent = new JsonHttpContent(jacksonFactory, getPayLoad());

                jsonHttpContent.setWrapperKey("instances");
                HttpRequest request = requestFactory.buildPostRequest(url, jsonHttpContent);

                LOG.info("Executing request...");
                HttpResponse response = request.execute();
                String content = response.parseAsString();

                LOG.info("Got the following response from CloudML \n" + content);
            } catch (Exception e) {
                LOG.error("Error while executing CloudML", e);
                e.printStackTrace();
                throw new RuntimeException("Not able to execute CloudML", e);
            }
        }

        private List<Map<String, Object>> getPayLoad() {
            // TODO - This has to be fetched from element instead of hardcoding
            List<Map<String, Object>> instances = new ArrayList<>();
            Map<String, Object> map = new HashMap<>();
            map.put("age", 25);
            map.put("workclass", " Private");
            map.put("education", " 11th");
            map.put("education_num", 7);
            map.put("marital_status", " Never-married");
            map.put("occupation", " Machine-op-inspct");
            map.put("relationship", " Own-child");
            map.put("race", " Black");
            map.put("gender", " Male");
            map.put("capital_gain", 0);
            map.put("capital_loss", 0);
            map.put("hours_per_week", 40);
            map.put("native_country", " United-States");

            instances.add(map);
            return instances;
        }
    }

    private static class ConstructLatLong extends SimpleFunction<TableRow, KV<LatLon, TableRow>> {

        @Override
        public KV<LatLon, TableRow> apply(TableRow t) {
            float lat = getRandomManhattanLat();
            float lon = getRandomManhattanLon();
            LatLon key = new LatLon(lat, lon);
            LOG.info("Using lat long " + key);

            return KV.of(key, t);
        }

        private float getRandomManhattanLat() {
            int randomCo = ThreadLocalRandom.current().nextInt(699, 720 + 1);
            return (40 + ((float) randomCo / (float) 1000));
        }

        private float getRandomManhattanLon() {
            int randomCo = ThreadLocalRandom.current().nextInt(976, 1016 + 1);
            return -(73 + ((float) randomCo / (float) 1000));
        }
    }

    private static class FilterOnCampaign extends DoFn<TableRow, TableRow> {
        FilterOnCampaign() {
        }

        @Override
        public void processElement(ProcessContext c) {
            TableRow ride = c.element();

            // Filtering based on Campaign
            // This can also be implemented using campaign category
            String campaign = ride.get("campaign").toString();
            LOG.info("Got the campaign " + campaign);
            if (FILTER_CAMPAIGN.equals(campaign)) {
                c.output(ride);
                LOG.info("Including rides of campaign " + campaign);
            } else {
                LOG.debug("Ignored campaign " + campaign);
            }
        }
    }

    private static class FilterOnCampaignCategory extends DoFn<TableRow, TableRow> {
        FilterOnCampaignCategory() {
        }

        @Override
        public void processElement(ProcessContext c) {
            TableRow ride = c.element();

            // Filtering based on Campaign
            // This can also be implemented using campaign
            String category = ride.get("campaign_category").toString();
            LOG.info("Got the category " + category);
            if (FILTER_CATEGORY.equals(category)) {
                c.output(ride);
                LOG.info("Including rides of category " + category);
            } else {
                LOG.debug("Ignored category " + category);
            }
        }
    }

    private static class FilterOnLatLon extends DoFn<TableRow, TableRow> {
        FilterOnLatLon() {
        }

        @Override
        public void processElement(ProcessContext c) {
            TableRow ride = c.element();

            // Filtering based on Campaign
            // This can also be implemented using campaign
            float lat = Float.parseFloat(ride.get("latitude").toString());
            float lon = Float.parseFloat(ride.get("longitude").toString());

            if (lon > -74.747 && lon < -73.969 && lat > 40.699 && lat < 40.720) {
                c.output(ride);
                LOG.info("Ride accepted : " + ride.get("passenger_count"));
            } else {
                LOG.info("Rejected ride lat: " + lat + " long:" + lon);
            }
        }
    }

    private static class LatestPointCombine extends Combine.CombineFn<TableRow, RidePoint, TableRow> {

        public RidePoint createAccumulator() {
            return new RidePoint();
        }

        public RidePoint addInput(RidePoint latest, TableRow input) {
            RidePoint newPoint = new RidePoint(input);
            if (latest.rideId == null || newPoint.timestamp > latest.timestamp) return newPoint;
            else return latest;
        }

        public RidePoint mergeAccumulators(Iterable<RidePoint> latestList) {
            RidePoint merged = createAccumulator();
            for (RidePoint latest : latestList) {
                if (merged.rideId == null || latest.timestamp > merged.timestamp)
                    merged = new RidePoint(latest);
            }
            return merged;
        }

        public TableRow extractOutput(RidePoint latest) {
                return latest.toTableRow();
            }
    }

    private static TableSchema getMergedSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();

        fields.add(new TableFieldSchema().setName("ride_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("point_idx").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("longitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        fields.add(new TableFieldSchema().setName("meter_reading").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("meter_increment").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("ride_status").setType("STRING"));
        fields.add(new TableFieldSchema().setName("passenger_count").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("user_liked_ad").setType("BOOLEAN"));
        fields.add(new TableFieldSchema().setName("campaign").setType("STRING"));
        fields.add(new TableFieldSchema().setName("campaign_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("campaign_category").setType("STRING"));
        fields.add(new TableFieldSchema().setName("dest_latitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("dest_longitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("route_info").setType("STRING"));

        return new TableSchema().setFields(fields);
        }

    public static void main(String args[]) {
        CustomPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollectionView<Map<String, Campaign>> campaignData = p.apply(BigQueryIO.Read.named("read campaign from BigQuery")
                .from(options.getCampaignTable()))
                .apply("Convert Campaign to KV", MapElements.via(new TransformCampaign()))
                .apply(View.asMap());
//        LOG.info("Created sideInput", campaignData.getName());

        PCollection<TableRow> datastream = p.apply(PubsubIO.Read.named("read from PubSub")
                .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
                .timestampLabel("ts")
                .withCoder(TableRowJsonCoder.of()))

                //Merging Campaign data
                .apply(ParDo.named("Merge Campaign")
                        .withSideInputs(campaignData)
                        .of(new DoFn<TableRow, TableRow>() {
                            @Override
                            public void processElement(ProcessContext c) throws Exception {
//                                LOG.info("Processing Input ", c.element());
                                TableRow tableRow = c.element();
//                                LOG.info("Fetching SideInput");
                                try {
                                    Map<String, Campaign> campaignMap = c.sideInput(campaignData);
                                    LOG.debug("Campaign Map : " + campaignMap);
                                    String campaignId = tableRow.get("campaign").toString();
                                    Campaign campaign= campaignMap.get(campaignId);

                                    tableRow.set("campaign_category", campaign.category);
                                    tableRow.set("campaign_name", campaign.name);
                                } catch (Exception e) {
                                    LOG.error("Error while getting Category", e);
                                }

                                c.output(tableRow);
                            }
                        }));


        String predictRestUrl = options.getPredictRestUrl();
        // Getting the latest rides alone
        datastream.apply("key rides by rideid",
                MapElements.via((TableRow ride) -> KV.of(ride.get("ride_id").toString(), ride))
                        .withOutputType(new TypeDescriptor<KV<String, TableRow>>() {
                        }))
                .apply("session windows on rides with early firings",
                        Window.<KV<String, TableRow>>into(
                                Sessions.withGapDuration(Duration.standardMinutes(60)))
                                .triggering(
                                        AfterWatermark.pastEndOfWindow()
                                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2000))))
                                .accumulatingFiredPanes()
                                .withAllowedLateness(Duration.ZERO))
                .apply("group ride points on same ride", Combine.perKey(new LatestPointCombine()))
                .apply("discard key",
                        MapElements.via((KV<String, TableRow> a) -> a.getValue())
                                .withOutputType(TypeDescriptor.of(TableRow.class)))

//                .apply("Call Cloud ML", ParDo.of(new CloudML(predictRestUrl)))
                //Writing lates taxi ride details with campaign data to BigQuery
                .apply(BigQueryIO.Write.named("Write To BigQuery")
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withSchema(getMergedSchema())
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .to(options.getRideDetailsTable()));

        // Writing the whole data into PubSub
        // For Visualization 1
        datastream.apply("window 1", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                    .apply("Mark rides", MapElements.via(new MarkRides()))
                    .apply("Count similar", Count.perKey())
                    .apply("Format rides", MapElements.via(new TransformRides()))
                    .apply(PubsubIO.Write.named("Write to PubSub")
                            .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic()))
                            .withCoder(TableRowJsonCoder.of()));

        // Filtering data to campaign and using specific lat|long and pushing it to PubSub
        // For Visualization 2
        datastream.apply("window 2", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
//                    .apply("Filtering rides based on Campaign", ParDo.of(new FilterOnCampaign()))
                    .apply("Filtering rides based on Campaign", ParDo.of(new FilterOnCampaignCategory()))
                    .apply("Filter rides based on lat/long", ParDo.of(new FilterOnLatLon()))
                    .apply(PubsubIO.Write.named("write to PubSub")
                            .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getFilteredSinkTopic()))
                            .withCoder(TableRowJsonCoder.of()));

        // Filtering data to specific lat|long and pushing it to PubSub
        // For Visualization 3
        datastream.apply("window 3", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply("Filtering rides based on Campaign", ParDo.of(new FilterOnCampaignCategory()))
                .apply(PubsubIO.Write.named("write to PubSub")
                        .topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getCampaignSinkTopic()))
                        .withCoder(TableRowJsonCoder.of()));

        p.run();
    }

}
