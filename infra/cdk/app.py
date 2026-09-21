#!/usr/bin/env python3
"""CDK application for the platform services stack."""

import os

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct

LAMBDA_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "functions")


class PlatformStack(Stack):
    """SNS routing, inbound webhooks, audit replay, and tenant provisioning."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.preference_table = dynamodb.Table(
            self,
            "NotificationPreferences",
            table_name="notification-preferences",
            partition_key=dynamodb.Attribute(
                name="user_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.partner_table = dynamodb.Table(
            self,
            "WebhookPartners",
            table_name="webhook-partners",
            partition_key=dynamodb.Attribute(
                name="partner_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.dedupe_table = dynamodb.Table(
            self,
            "WebhookDedupe",
            table_name="webhook-dedupe",
            partition_key=dynamodb.Attribute(
                name="dedupe_key", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.tenant_table = dynamodb.Table(
            self,
            "Tenants",
            table_name="tenants",
            partition_key=dynamodb.Attribute(
                name="tenant_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.router_topic = sns.Topic(
            self, "NotificationRouterTopic", topic_name="notification-router"
        )
        self.audit_bus = events.EventBus(self, "AuditBus", event_bus_name="audit")

        self.ingest_queue = sqs.Queue(
            self,
            "WebhookIngestQueue",
            queue_name="webhook-ingest",
            visibility_timeout=Duration.seconds(300),
        )
        self.provisioning_queue = sqs.Queue(
            self,
            "TenantProvisioningQueue",
            queue_name="tenant-provisioning",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=sqs.Queue(
                    self,
                    "ProvisioningRedriveTarget",
                    queue_name="provisioning-redrive-dlq",
                    retention_period=Duration.days(14),
                ),
            ),
        )

        # DLQs
        self.platform_dlq = sqs.Queue(
            self,
            "PlatformDLQ",
            queue_name="platform-dlq",
            retention_period=Duration.days(14),
        )

        self.provisioning_dlq = sqs.Queue(
            self,
            "ProvisioningDLQ",
            queue_name="provisioning-dlq",
            retention_period=Duration.days(14),
        )

        self.data_bucket = s3.Bucket(
            self,
            "TenantDataBucket",
            bucket_name="tenant-data",
            removal_policy=RemovalPolicy.RETAIN,
        )

        guard_env = {
            "MAX_PAYLOAD_SIZE_BYTES": "262144",
            "MAX_INVOCATION_DEPTH": "3",
            "MAX_LOOP_ITERATIONS": "1000",
            "MAX_RETRIES": "3",
            "MAX_BACKOFF_SECONDS": "10.0",
            "MAX_PAGINATION_PAGES": "100",
            "MIN_REMAINING_MS": "5000",
        }

        # ------------------------------------------------------------------
        # notification_router -- SNS triggered, republishes to the same topic
        # ------------------------------------------------------------------
        notification_router = self._function(
            "NotificationRouter",
            "notification_router",
            timeout=60,
            environment={
                "PREFERENCE_TABLE": self.preference_table.table_name,
                "ROUTER_TOPIC_ARN": self.router_topic.topic_arn,
                **guard_env,
            },
        )
        self.router_topic.add_subscription(
            subscriptions.LambdaSubscription(notification_router)
        )
        # Async retry bounds for SNS-triggered function
        lambda_.EventInvokeConfig(
            self,
            "NotificationRouterRetry",
            function=notification_router,
            max_event_age=Duration.minutes(5),
            retry_attempts=1,
            on_failure=lambda_.destinations.SqsDestination(self.platform_dlq) if hasattr(lambda_, "destinations") else None,
        )
        self.router_topic.grant_publish(notification_router)
        self.preference_table.grant_read_data(notification_router)

        # ------------------------------------------------------------------
        # webhook_relay -- Function URL, no API Gateway in front
        # ------------------------------------------------------------------
        webhook_relay = self._function(
            "WebhookRelay",
            "webhook_relay",
            timeout=25,
            environment={
                "PARTNER_TABLE": self.partner_table.table_name,
                "DEDUPE_TABLE": self.dedupe_table.table_name,
                "INGEST_QUEUE_URL": self.ingest_queue.queue_url,
                **guard_env,
            },
        )
        webhook_relay.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )
        # Reserved concurrency for Function URL -- sole throttle
        lambda_.CfnFunction.override_logical_id
        cfn_fn = webhook_relay.node.default_child
        cfn_fn.add_property_override("ReservedConcurrentExecutions", 50)
        self.partner_table.grant_read_data(webhook_relay)
        self.dedupe_table.grant_read_write_data(webhook_relay)
        self.ingest_queue.grant_send_messages(webhook_relay)

        # ------------------------------------------------------------------
        # audit_event_replay -- EventBridge, re-puts to the same bus
        # ------------------------------------------------------------------
        audit_replay = self._function(
            "AuditEventReplay",
            "audit_event_replay",
            timeout=60,
            environment={
                "AUDIT_BUS_NAME": self.audit_bus.event_bus_name,
                **guard_env,
            },
        )
        # Async retry config
        lambda_.EventInvokeConfig(
            self,
            "AuditReplayRetry",
            function=audit_replay,
            max_event_age=Duration.minutes(5),
            retry_attempts=1,
            on_failure=lambda_.destinations.SqsDestination(self.platform_dlq) if hasattr(lambda_, "destinations") else None,
        )
        events.Rule(
            self,
            "AuditReplayRule",
            event_bus=self.audit_bus,
            event_pattern=events.EventPattern(
                source=["audit.replay"],
                detail_type=["replay.request", "replay.continuation"],
            ),
            targets=[targets.LambdaFunction(audit_replay)],
        )
        self.audit_bus.grant_put_events_to(audit_replay)

        # ------------------------------------------------------------------
        # tenant_provisioner -- API Gateway AND SQS
        # ------------------------------------------------------------------
        tenant_provisioner = self._function(
            "TenantProvisioner",
            "tenant_provisioner",
            timeout=25,
            environment={
                "TENANT_TABLE": self.tenant_table.table_name,
                "DATA_BUCKET": self.data_bucket.bucket_name,
                "ONBOARDING_QUEUE_URL": self.provisioning_queue.queue_url,
                **guard_env,
            },
        )
        api = apigw.RestApi(self, "PlatformApi", rest_api_name="platform",
            deploy_options=apigw.StageOptions(
                throttling_burst_limit=100,
                throttling_rate_limit=50,
            ),
        )
        tenant_model = api.add_model(
            "TenantRequestModel",
            content_type="application/json",
            model_name="TenantRequestModel",
            schema=apigw.JsonSchema(
                schema=apigw.JsonSchemaVersion.DRAFT4,
                type=apigw.JsonSchemaType.OBJECT,
                required=["tenant_name", "admin_email"],
                properties={
                    "tenant_name": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING,
                        max_length=128,
                    ),
                    "admin_email": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING,
                        max_length=256,
                    ),
                    "plan": apigw.JsonSchema(
                        type=apigw.JsonSchemaType.STRING,
                        enum=["free", "starter", "business", "enterprise"],
                    ),
                },
            ),
        )
        request_validator = api.add_request_validator(
            "BodyValidator",
            validate_request_body=True,
        )
        api.root.add_resource("tenants").add_method(
            "POST",
            apigw.LambdaIntegration(tenant_provisioner),
            request_models={"application/json": tenant_model},
            request_validator=request_validator,
        )
        tenant_provisioner.add_event_source(
            sources.SqsEventSource(
                self.provisioning_queue,
                batch_size=10,
                report_batch_item_failures=True,
                max_concurrency=10,
            )
        )
        self.tenant_table.grant_read_write_data(tenant_provisioner)
        self.data_bucket.grant_read_write(tenant_provisioner)

    def _function(
        self, construct_id: str, module: str, environment=None, memory_size: int = 512,
        timeout: int = 30,
    ) -> lambda_.Function:
        return lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(os.path.join(LAMBDA_ROOT, "platform")),
            handler="{0}.lambda_handler".format(module),
            memory_size=memory_size,
            timeout=Duration.seconds(timeout),
            log_retention=logs.RetentionDays.ONE_MONTH,
            environment=dict(environment or {}, LOG_LEVEL="WARNING"),
        )


app = cdk.App()
PlatformStack(app, "PlatformStack")
app.synth()
