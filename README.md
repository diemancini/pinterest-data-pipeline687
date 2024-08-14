# PINTEREST-DATA-PIPELINE687

## TABLE OF CONTENTS

1. [A DESCRIPTION OF THE PROJECT](#description)

2. [INSTALLATION INSTRUCTIONS](#install)

   2.1 [Python and git](#python_git)

   2.2 [Packages](#packages)

3. [PREPARING THE ENVIRONMENT FOR AWS](#preparing_enviroment_aws)

   3.1 [Connect to the EC2 instance](#connect_ec2)

   3.2 [Set up Kafka on the EC2 instance](#setup_kafka)

   3.3 [Create a custom plugin with MSK Connect in S3](#create_custom_plugin)

   3.4 [Create the connector with MKS Connector ](#create_mks_connector)

   3.5 [Create a kafka REST proxy integration for the API ](#create_kafka_rest_api)

4. [SEND DATA TO API](#send_data_to_api)

5. [BATCH PROCESSING: DATABRICKS](#bp_databricks)

6. [BATCH PROCESSING: DATABRICKS AND SPARKS](#bp_databricks_sparks)

7. [BATCH PROCESSING: AWS MWAA](#bp_aws_mwaa)

8. [Set up ](#setup_)

## <a id="description">A DESCRIPTION OF THE PROJECT</a>

It is in progress.....

## <a id="install">INSTALLATION INSTRUCTIONS</a>

### <a id="python_git">Python and git</a>

Python has to be installed for running this application. Follow these steps below:

- Install [python 3](https://www.python.org/downloads/) accordingly with your OS.
- Clone repository with

```
git clone https://github.com/diemancini/pinterest-data-pipeline687.git
```

### <a id="packages">Packages</a>

For running this application, it must to be installed:

- boto3==1.34
- PyYAML==6.0.1
- requests==2.32.3
- SQLAlchemy==2.0.31
- PyMySQL==1.1.1

## <a id="preparing_enviroment_aws">PREPARING THE ENVIRONMENT FOR AWS</a>

### <a id="connect_ec2">Connect to the EC2 instance</a>

There is a documentation in this page: [Connect to your instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html?icmpid=docs_ec2_console#ec2-connect-to-instance).

For connection to ec2 virtual machine that the AICore already created for you, it must to prepare local environment first:

- Creating a .pem file which must contains the "RSA fingerprint" stored in AWS Parameter Store.
- Using this command:

```
ssh -i {path/to/pem/file}.pem ec2-user@{ip_address}.compute-1.amazonaws.com
```

Example in my case:

```
ssh -i config/0affcd87e38f-key-pair.pem ec2-user@ec2-35-173-132-69.compute-1.amazonaws.com
```

If you done correctly, you shoud see this screen below:

```
  Last login: Thu Aug  1 09:56:07 2024 from 45.144.57.69

       __|  __|_  )
       _|  (     /   Amazon Linux 2 AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-2/
55 package(s) needed for security, out of 90 available
Run "sudo yum update" to apply all updates.
```

### <a id="setup_kafka">Set up Kafka on the EC2 instance</a>

There is a documentation in this page: [Create client machine and Apache Kafka topic](https://docs.aws.amazon.com/msk/latest/developerguide/mkc-create-topic.html).
After you accessed the EC2 instance remotely, just follow these steps below:

- Install Java on the client instance by running the following command:

```
sudo yum install java-1.8.0

```

- Run the following command to download Apache Kafka.

```
  wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz

```

- Run the following command in the directory where you downloaded the TAR file in the previous step.

```
tar -xzf kafka_2.12-2.2.1.tgz

```

- Go to the kafka_2.12-2.8.1 directory.
- Go to the bin directory and create a client.properties file with this content below:

```
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler

```

- Download the IAM MSK authentication package from Github, using the following command:

```
wget https://github.com/aws/aws-msk-iam-auth/releases/tag/v2.2.0/aws-msk-iam-auth-2.1.0-all.jar

```

- After you have downloaded the package above, move it into libs kafka folder.

- Create a environment variable called CLASSPATH with this command:

```
export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-2.1.0-all.jar
```

You can setting up this command in your .bashrc file, located in ec2 instance home. If you do that,
you'll persit this variable, without setting this up everytime the instance restarts. Run:

```
nano ~/.bashrc.
```

And add the export command above at the end of the file and save it.

- Follow the "step 3" of task 3 MILESTONE 3 for adding principal in Edit trust policy (IAM Roles).

- Run the following command on the client instance (mkc-tutorial-client), replacing bootstrapServerString with the value that you saved when you viewed the cluster's client information.

```
<path-to-your-kafka-installation>/bin/kafka-topics.sh --create --bootstrap-server bootstrapServerString --topic {name_of_the_topic}

```

The name of the topic MUST be what the task 4 in MILESTONE 3 described, for instance:

```
<your_UserId>.pin for the Pinterest posts data

```

Otherwise, you can't create any other topic, since the account of this project does not have admin privilegies. I spent sometime
for figure out that, discussing with one of the tutors engineers (with Blair). Thanks a lot! :)

### <a id="create_custom_plugin">Create a custom plugin with MSK Connect in S3</a>

This can be seen in "MSK Connect" class on "AWS Data Engineering Services" AICore module.
In order to create a custom plugin, first you have to connect in your EC2 instance.

- After that, use these commands below in ec2 instance.

```
# assume admin user privileges
sudo -u ec2-user -i
# create directory where we will save our connector
mkdir kafka-connect-s3 && cd kafka-connect-s3
# download connector from Confluent
wget https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.5.13/confluentinc-kafka-connect-s3-10.5.13.zip
# copy connector to our S3 bucket
aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
```

The link address on wget could be different that appears here. So check the right link at [confluent.io website](https://www.confluent.io/hub/confluentinc/kafka-connect-s3?session_ref=direct).

- If everything ran successfully, you should be able to see the "kafka-connect-s3/" inside your S3 bucket.
  Now go to "MSK Connect" section on the left side of the console. Choose "Create customized plugin".
- On this page, in the field "S3 URI - Custom plugin object", put the url of the bucket that contains the connector whichc you created earlier with .zip file format. In "Custom plugin name", put the name <your_UserId>-plugin.
- Click on "Create custom plugin".

### <a id="create_mks_connector">Create the connector with MKS Connector</a>

This can be seen in "MSK Connect" class on "AWS Data Engineering Services" AICore module.

- Still in MKS Connect page, choose "Create connector".
- In the list of plugin, select the plugin you have just created (user-<your_UserId>-bucket), and then click "Next".
- In the Connector configuration settings copy the following configuration:

```
connector.class=io.confluent.connect.s3.S3SinkConnector
# same region as our bucket and cluster
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
# include nomeclature of topic name, given here as an example will read all data from topic names starting with msk.topic....
topics.regex=<YOUR_UUID>.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<BUCKET_NAME>
```

Just replace <YOUR_UUI> and <BUCKET_NAME> by your credentials.
Leave the rest of the configurations as default, except for:

- Connector type change to Provisioned and make sure both the MCU count per worker and Number of workers are set to 1
- Worker Configuration, select Use a custom configuration, then pick confluent-worker
- Access permissions, where you should select the IAM role you have created previously

Skip the next steps and you should have created a new connector.

### <a id="create_kafka_rest_api">Create Kafka REST proxy integration for the API</a>

#### Create Resource and Stages

This can be seen in "Integrating API Gateway with Kafka" class on "AWS Data Engineering Services" AICore module.
First of all, you have to create a "resource" AWS Gateway API feature. In order to do that, follow these steps below:

- Enter in your API Gateway console, on the left side bar, click in "Resources" and click in "Create resouces".
- The next screen shows two fields: "Resource Path" and "Resource Name". Select the Proxy resource toogle.
  For Resource Name enter {proxy+}. Finally, select Enable API Gateway CORS and choose Create Resource.
- After create the resource, click in "Deploy API". You should see a new page with "Stage" dropdown. If you don't have
  any stage created, choose "New stage" and put a name of this current stage. The name of the stage will be your url path.

Now we have to create a proxy in order to comunicate with the EC2 instance, which contains the Kafka Rest Server (we will build this later).

- On Resource section, click in "ANY" on {proxy+} level in "Resources" side bar.
- Click on "Integration request" tab and "Edit".
- For HTTP method select ANY.
- For the Endpoint URL, you will be your EC2 Instance PublicDNS. You can obtain your EC2 Instance Public DNS by navigating to the EC2 console. The endpoint URL should have the following format: http://KafkaClientEC2InstancePublicDNS:8082/{proxy}. The 8082 port is a default port for Kafka Rest Server.
- Click in "HTTP proxy integration" slide button if is not turn it on.
- Click in "Save".
- Deploy API again.

Make a note of the Invoke URL in Stage section.

#### Create Kafka REST proxy in EC2 client instance

For the API to communicate through the server (EC2 client instance), we need to create a proxy server. To achieve that, it will install the Confluent package for the Kafka REST Proxy.

- Inside of the EC2 client instance, run this commands below:

```
sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz
tar -xvzf confluent-7.2.0.tar.gz
```

- To configure the REST proxy to communicate with the desired MSK cluster, and to perform IAM authentication you first need to navigate to confluent-7.2.0/etc/kafka-rest. Inside here run the following command to modify the kafka-rest.properties file:

```
nano kafka-rest.properties
```

- Change the bootstrap.servers and the zookeeper.connect variables inside of this file, adding these lines below as well:

```
client.security.protocol = SASL_SSL
client.sasl.mechanism = AWS_MSK_IAM
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

- Deploy the API again on Gateway API.
- Starting the REST proxy running on confluent-7.2.0/bin folder:

```
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties

```

If everything went well, you should see: "INFO Server started, listening for requests..." in your EC2 console.

## <a id="send_data_to_api">SEND DATA TO API</a>

We prepared the environment to create data through the API (via Gateway API in AWS), which will use producers in EC2 client instance, using Kafka REST Proxy.

Now that can be used with python script. In the existing user_posting_emulation.py file, I added http(data, method) method to create new data from database of pin, geo and user tables (I will update this function later).

Execute this command below to run the API:

```
python3 user_posting_emulation.py
```

If everything is working, the data of pin should appear in your S3 bucket.
I had some issues regarding the connections between API Gateway and Kafka Rest Proxy. Wayne and Blair helped me out on that. Thanks a lot again. :). The issues was:

- The Invoke url was responding 500 status code, related to SSL certificate. Since it does not have SSL certificate setup on that end point, we simply changed to http instead of https.
- The format of data that we send to the topics, should be like this:

```
{"records": {
    "value": data
  }
}
```

Where "data" is the data that we must to send. This requirement is mandatory by Confluent package (Kafka Rest Proxy).

- One of the engineers had to rebuild the connector (just for safety reasons) and clean the old topics, which I ran it with the producer command in EC2 client instance directly, before of build the kafka rest proxy and gateway api.

## <a id="bp_databricks">BATCH PROCESSING: DATABRICKS</a>

In order to process the raw data, we have to clean it. Databricks is AWS platform that has several tools to manage the data (store, build scripts to deal with it, etc).\
In this part of the project, we want to retrieve the data from S3 bucket that we built earlier and convert into a Dataframe. To achieve that, Databricks has
a tool called "Notebook".\
 Notebook supports python scripts, already has several packages pre installed and uses cells (like Notebook in jupiter) to write python scripts.
We gonna use the Notebook for:

- Retrieve the credentials in order to communitate to S3 bucket

```
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)
# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
```

- Create the drive and retrieve the data from S3 bucket.

```
# AWS S3 bucket name
AWS_S3_BUCKET = "user-0affcd87e38f-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/s3_0affcd87e38f-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
```

- Get data from drive for given topic and convert into a Dataframe.

```
## Disable format checks during the reading of Delta tables
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
def get_dataframe_from_drive(topic):
    # File location and type
    # Asterisk(*) indicates reading all the content of the specified file that have .json extension
    file_location = f"/mnt/s3_0affcd87e38f-bucket/topics/{topic}/partition=0/*.json"
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

    return df
```

- Use the function for retrieve data for each topic.

```
df_pin = get_dataframe_from_drive("0affcd87e38f.pin")
df_geo = get_dataframe_from_drive("0affcd87e38f.geo")
df_user = get_dataframe_from_drive("0affcd87e38f.user")

```

### <a id="bp_databricks_sparks">BATCH PROCESSING: DATABRICKS AND SPARKS</a>

On the last section, the data that is stored in S3 bucket was brought into Databricks using Notebook tool and pyspark packages as well.\
Now is necessary to prepare this data to save them properly in Databricks. To achieve that, we will use again the sparks library, for
pre processing (clean it accordingly with client requirements).\
Pyspark library has a powerful features, including load this data as
Dataframe, which is pretty similar to pandas library.\
Thought, it is necessary to execute some commands that reminds a little bit a
sql queries, such as Select, join, etc (Which is awesome). The documentation is [here](https://spark.apache.org/docs/latest/api/python/index.html).

About using the spark in this part of the project, I used these commands:

- select(column name 1, column name 2, ...>)\
  It is similar to SELECT statement of SQL.

- withColumn(column name, value or new value | conditionals))\
  It is action that what value you want to change in each row for given column name.
  The conditional can be used as well, mostly "when".

- when(conditional, value)\
  It is similar to "Where" clause in SQL. Could be chained with "otherwise" in "value" field.

- otherwise (conditional, value)\
  As the name suggests, it is equivalent to "else" in "if" statement of most popular languages.
  The most conditional used is "when", which reminds the subqueries of SQL.

- groupBy(column name 1, column name 2, ...)\
  Group the table by given column(s) name(s)

- agg(function)\
  This is aggregate function uses others aggregations functions, like "count()", "sum()", "avg()", etc.
  As in SQL, it must be used after groupBY.

- count()\
  Function that counts of occurrencies of particular column that it was used in groupBy.

- orderBy(column name, value or new value)\
  Order the result query by columns names sequence.

- F\
  Contains several functions of pyspark library (e.g. mean()).

You can see how I used these functions for this project in [databricks_notebooks](databricks_notebooks) folder.

### <a id="bp_aws_mwaa">BATCH PROCESSING: AWS MWAA</a>

For running the scheduled tasks in databricks, AWS has a tool called "Amazon Managed Workflows for Apache Airflow" (AKA [MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/)).\
There is a tutorial to setup the MWAA environment integraded with S3 storage in AWS on "AWS Data Engineering Services" module AICore website.
Since this project gave to us the enviroment already setup, I just to had to:

- Create a python file called [0affcd87e38f_dag.py](./dags/0affcd87e38f_dag.py), uploaded in S3 bucket called "mwaa-dags-bucket" of folder dags.
- This file contains just one task and I setup the fields:
  - notebook_path
  - owner
  - dag_id (first parameter in DAG class)
  - schedule_interval
  - start_date
  - existing_cluster_id
- Opened the Airflow UI and ran the task manually.

### <a id=""></a>

```

```

### <a id=""></a>

```

```
