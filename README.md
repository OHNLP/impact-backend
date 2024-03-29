# IMPACT-Backend
This component provides query execution and patient retrieval scoring for IMPACT

## Building

You will require Java 9+ and maven. Additionally, you will need an execution engine installed. Supported execution engines include Apache Flink, Spark, and GCP Dataflow. Please note that your execution nodes will need to have access to your data sources

To compile, execute `git clone https://github.com/OHNLP/impact-backend.git`, modify `src/main/resources/config.template.json` with your data connection details and rename to `config.json`, followed by `mvn clean install` inside the cloned directory.

For testing purposes, a sample criteria much like what would be generated from the frontend component can be found [here](https://github.com/OHNLP/impact-backend/blob/master/src/main/resources/sample_criterion.json) 

