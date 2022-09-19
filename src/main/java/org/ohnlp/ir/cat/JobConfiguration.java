package org.ohnlp.ir.cat;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.UUID;

public interface JobConfiguration extends PipelineOptions {
    @Description("The Middleware Job UID associated with this run")
    UUID getJobid();
    void setJobid(UUID jobid);

    @Description("The Middleware Server callback URL")
    String getCallback();
    void setCallback(String callback);
}
