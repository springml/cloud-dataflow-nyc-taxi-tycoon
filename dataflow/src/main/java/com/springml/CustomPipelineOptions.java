package com.springml;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.options.*;

/**
 * Options class for SML TaxiGDF
 */
public interface CustomPipelineOptions extends DataflowPipelineOptions {
    @Description("ProjectId where data source topic lives")
    @Default.String("billion-taxi-rides")
//    @Default.String("pubsub-public-data")
    @Validation.Required
    String getSourceProject();

    void setSourceProject(String value);

    @Description("TopicId of source topic")
    @Default.String("smlfeed")
//    @Default.String("taxirides-realtime")
    @Validation.Required
    String getSourceTopic();

    void setSourceTopic(String value);

    @Description("ProjectId where data sink topic lives")
    @Validation.Required
    String getSinkProject();

    void setSinkProject(String value);

    @Description("TopicId of sink topic")
    @Default.String("smlvisualizer")
    @Validation.Required
    String getSinkTopic();

    void setSinkTopic(String value);

    @Description("TopicId of Filtered sink")
    @Default.String("smlgdf2")
    @Validation.Required
    String getFilteredSinkTopic();

    void setFilteredSinkTopic(String value);

    @Description("TopicId of Campaign sink")
    @Default.String("smltarget3")
    @Validation.Required
    String getCampaignSinkTopic();

    void setCampaignSinkTopic(String value);

    @Description("Fully qualified campaign table name")
    @Default.String("billion-taxi-rides:advertising.campaign")
    @Validation.Required
    String getCampaignTable();

    void setCampaignTable(String value);

    @Description("Fully qualified ride details table name")
    @Default.String("billion-taxi-rides:advertising.ride_details")
    @Validation.Required
    String getRideDetailsTable();

    void setRideDetailsTable(String value);
}
