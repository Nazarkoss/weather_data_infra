from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_logs as logs,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_glue as glue,
    aws_ecr_assets as ecr_assets,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as _alambda,
    aws_lambda_event_sources as eventsources
)
from constructs import Construct

class WeatherDataInfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines the stack

        # TODO: Create an ECR repository and push the image to it
        #       - Currently impossible, see: https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_ecr_assets/README.html#publishing-images-to-ecr-repositories

        # Define a Docker image asset
        docker_image_asset = ecr_assets.DockerImageAsset(self,
            directory=".",  # Directory containing the Dockerfile
            file="./image/Dockerfile",
            **config.get("ecr_docker_image", {})
        )

        # Create an S3 bucket
        bucket = s3.Bucket(
            self,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            **config.get("s3_bucket", {})
        )

        # Define an IAM role for the crawler
        glue_role = iam.Role(
            self,
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
            **config.get("iam_role_glue", {})
        )
        
        # Add an inline policy for S3 access
        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            resources=[bucket.bucket_arn + "/*"],  # Adjust the bucket name accordingly
            effect=iam.Effect.ALLOW
        ))

        # Define the Glue Crawler
        crawler = glue.CfnCrawler(
            self,
            role=glue_role.role_arn,
            targets={
                "s3Targets": [{
                    "path": f"s3://{bucket.bucket_name}/raw/data-around-france/"
                }]
            },
            # Configure additional settings as needed
            schema_change_policy={
                "deleteBehavior": "DEPRECATE_IN_DATABASE",
                "updateBehavior": "UPDATE_IN_DATABASE"
            },
            **config.get("glue_crawler", {})
        )

        # SQS Queue
        queue = sqs.Queue(
            self,
            visibility_timeout=Duration.seconds(300),
            **config.get("sqs_queue", {})
        )
        
        # Create a CloudWatch log group for the Lambda function
        log_group = logs.LogGroup(
            self, 
            log_group_name=f"/aws/lambda/{config["docker_image_lambda_function"]["function_name"]}",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.ONE_WEEK,
            **config.get("cw_log_group", {})
        )

        # IAM role for the Lambda function
        lambda_role = iam.Role(self,
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               **config.get("iam_role_lambda", {})
        )

        # Policy to allow Lambda to access the S3 bucket
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=[bucket.bucket_arn + "/*"]
        ))

        # Policy to allow Lambda to write to CloudWatch Logs
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            resources=[log_group.log_group_arn]
        ))
        
        docker_image_lambda_function = _lambda.DockerImageFunction(
            self,
            code=_lambda.DockerImageCode.from_ecr(
                repository=docker_image_asset.repository, 
                tag=docker_image_asset.asset_hash  # or specify your custom tag,
            ),
            memory_size=512,  # Memory size in MB
            timeout=Duration.minutes(1),  # Timeout in seconds
            role=lambda_role,
            architecture=_lambda.Architecture.ARM_64,
            **config.get("docker_image_lambda_function", {})
        )
        
        queue.grant_consume_messages(docker_image_lambda_function)
        docker_image_lambda_function.add_event_source(
            eventsources.SqsEventSource(
                queue, 
                batch_size=3
            )
        )

        # Legacy way of pushing the data to S3 - abandonned to push parquet files directly
        # lambda_function = _alambda.PythonFunction(
        #     self,
        #     entry="./lambda/",
        #     runtime=_lambda.Runtime.PYTHON_3_11,
        #     index="weather_data_crawler.py",
        #     handler="handler",
        #     memory_size=256,  # Memory size in MB
        #     timeout=Duration.minutes(1),  # Timeout in seconds
        #     role=lambda_role,
        #     **config.get("lambda_function", {})
        # )
