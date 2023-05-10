# IMPACT-Backend
This provides query execution and patient retrieval scoring.

## Building

You will require Java 9+ and maven. Additionally, you will need an execution engine installed. Supported execution engines include Apache Flink, Spark, and GCP Dataflow. Please note that your execution nodes will need to have access to your data sources

To compile, execute `git clone https://github.com/OHNLP/impact-backend.git`, modify `src/main/resources/config.template.json` with your data connection details, followed by `mvn clean install` inside the cloned directory.


