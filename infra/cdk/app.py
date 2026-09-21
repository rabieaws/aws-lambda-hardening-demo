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
        )

        self.data_bucket = s3.Bucket(
            self,
            "TenantDataBucket",
            bucket_name="tenant-data",
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ------------------------------------------------------------------
        # notification_router -- SNS triggered, republishes to the same topic
        # ------------------------------------------------------------------
        notification_router = self._function(
            "NotificationRouter",
            "notification_router",
            environment={
                "PREFERENCE_TABLE": self.preference_table.table_name,
                "ROUTER_TOPIC_ARN": self.router_topic.topic_arn,
            },
        )
        self.router_topic.add_subscription(
            subscriptions.LambdaSubscription(notification_router)
        )
        self.router_topic.grant_publish(notification_router)
        self.preference_table.grant_read_data(notification_router)

        # ------------------------------------------------------------------
        # webhook_relay -- Function URL, no API Gateway in front
        # ------------------------------------------------------------------
        webhook_relay = self._function(
            "WebhookRelay",
            "webhook_relay",
            environment={
                "PARTNER_TABLE": self.partner_table.table_name,
                "DEDUPE_TABLE": self.dedupe_table.table_name,
                "INGEST_QUEUE_URL": self.ingest_queue.queue_url,
            },
        )
        webhook_relay.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,
        )
        self.partner_table.grant_read_data(webhook_relay)
        self.dedupe_table.grant_read_write_data(webhook_relay)
        self.ingest_queue.grant_send_messages(webhook_relay)

        # ------------------------------------------------------------------
        # audit_event_replay -- EventBridge, re-puts to the same bus
        # ------------------------------------------------------------------
        audit_replay = self._function(
            "AuditEventReplay",
            "audit_event_replay",
            environment={"AUDIT_BUS_NAME": self.audit_bus.event_bus_name},
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
            environment={
                "TENANT_TABLE": self.tenant_table.table_name,
                "DATA_BUCKET": self.data_bucket.bucket_name,
                "ONBOARDING_QUEUE_URL": self.provisioning_queue.queue_url,
            },
        )
        api = apigw.RestApi(self, "PlatformApi", rest_api_name="platform")
        api.root.add_resource("tenants").add_method(
            "POST", apigw.LambdaIntegration(tenant_provisioner)
        )
        tenant_provisioner.add_event_source(
            sources.SqsEventSource(self.provisioning_queue, batch_size=10)
        )
        self.tenant_table.grant_read_write_data(tenant_provisioner)
        self.data_bucket.grant_read_write(tenant_provisioner)

    def _function(
        self, construct_id: str, module: str, environment=None, memory_size: int = 512
    ) -> lambda_.Function:
        return lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(os.path.join(LAMBDA_ROOT, "platform")),
            handler="{0}.lambda_handler".format(module),
            memory_size=memory_size,
            environment=dict(environment or {}, LOG_LEVEL="INFO"),
        )


app = cdk.App()
PlatformStack(app, "PlatformStack")
app.synth()
