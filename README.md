# PINTEREST-DATA-PIPELINE687

## TABLE OF CONTENTS

1. [A DESCRIPTION OF THE PROJECT](#description)

2. [INSTALLATION INSTRUCTIONS](#install)

   2.1 [Python and git](#python_git)

   2.2 [Packages](#packages)

3. [PREPARING THE ENVIRONMENT FOR AWS](#preparing_enviroment_aws)

   3.1 [Connect to the EC2 instance](#connect_ec2)

   3.2 [Set up Kafka on the EC2 instance](#setup_kafka)

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

```

```
