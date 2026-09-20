#!/usr/bin/env python3
"""CDK app for the identity and IoT telemetry Lambda estates."""

import os
from typing import Any, Dict, Optional

import aws_cdk as cdk
from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_sqs as sqs
from constructs import Construct

LAMBDA_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "lambdas")


class IdentityStack(Stack):
    """API Gateway fronted identity functions plus the Cognito trigger."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.api = apigw.RestApi(
            self,
            "IdentityApi",
            rest_api_name="identity-api",
            deploy_options=apigw.StageOptions(stage_name="prod"),
        )

        self.session_table = dynamodb.Table(
            self,
            "RefreshTokens",
            partition_key=dynamodb.Attribute(
                name="token_handle", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.dlq = sqs.Queue(
            self,
            "IdentityDLQ",
            queue_name="identity-dlq",
            retention_period=Duration.days(14),
        )

        authorizer_fn = self._function("JwtCustomAuthorizer", "jwt_custom_authorizer")
        authorizer = apigw.RequestAuthorizer(
            self,
            "TokenAuthorizer",
            handler=authorizer_fn,
            identity_sources=[apigw.IdentitySource.header("Authorization")],
        )

        self._route("LoginAttemptEvaluator", "login_attempt_evaluator",
                    "login-attempts", "POST")
        self._route("SessionRefreshHandler", "session_refresh_handler",
                    "sessions-refresh", "POST")
        self._route("MfaChallengeVerifier", "mfa_challenge_verifier",
                    "mfa-verify", "POST", authorizer)
        self._route("UserProvisioningScim", "user_provisioning_scim",
                    "scim-users", "PATCH", authorizer)
        self._route("ConsentPreferenceCenter", "consent_preference_center",
                    "consents", "PUT", authorizer)
        self._route("DeviceTrustEvaluator", "device_trust_evaluator",
                    "devices-evaluate", "POST", authorizer)

        # Cognito pre-sign-up trigger, wired to the user pool in the identity account
        self._function("PasswordPolicyEnforcer", "password_policy_enforcer")

        # Invoked directly by the authorization service
        self._function("PermissionExpander", "permission_expander", memory_size=1024)

        rotation = self._function("ApiKeyRotationWorker", "api_key_rotation_worker")
        events.Rule(
            self,
            "ApiKeyRotationSchedule",
            schedule=events.Schedule.rate(Duration.hours(6)),
            targets=[targets.LambdaFunction(rotation)],
        )

    def _function(
        self,
        construct_id: str,
        module: str,
        memory_size: int = 512,
        environment: Optional[Dict[str, str]] = None,
    ) -> lambda_.Function:
        return lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset(os.path.join(LAMBDA_ROOT, "identity")),
            handler="{0}.lambda_handler".format(module),
            memory_size=memory_size,
            timeout=Duration.seconds(30),
            reserved_concurrent_executions=10,
            dead_letter_queue=self.dlq,
            environment=dict(environment or {}, LOG_LEVEL="INFO"),
        )

    def _route(
        self,
        construct_id: str,
        module: str,
        path: str,
        method: str,
        authorizer: Optional[apigw.IAuthorizer] = None,
    ) -> lambda_.Function:
        handler = self._function(construct_id, module)
        resource = self.api.root.add_resource(path)
        resource.add_method(
            method,
            apigw.LambdaIntegration(handler),
            authorizer=authorizer,
            authorization_type=(
                apigw.AuthorizationType.CUSTOM if authorizer else apigw.AuthorizationType.NONE
            ),
        )
        return handler


class IotTelemetryStack(Stack):
    """Kinesis and IoT driven telemetry processors."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.telemetry_stream = kinesis.Stream(
            self,
            "TelemetryStream",
            stream_name="device-telemetry",
            shard_count=8,
            retention_period=Duration.hours(48),
        )

        self.aggregate_table = dynamodb.Table(
            self,
            "WindowAggregates",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.dlq = sqs.Queue(
            self,
            "IotTelemetryDLQ",
            queue_name="iot-telemetry-dlq",
            retention_period=Duration.days(14),
        )

        for construct_id_, module in (
            ("KinesisTelemetryAggregator", "kinesis_telemetry_aggregator"),
            ("AnomalyThresholdDetector", "anomaly_threshold_detector"),
            ("SensorCalibrationDrift", "sensor_calibration_drift"),
            ("EnergyConsumptionRollup", "energy_consumption_rollup"),
        ):
            handler = self._function(construct_id_, module)
            handler.add_event_source(
                sources.KinesisEventSource(
                    self.telemetry_stream,
                    batch_size=1000,
                    starting_position=lambda_.StartingPosition.LATEST,
                )
            )

        # IoT Core rule actions, wired in the IoT account
        self._function("DeviceShadowReconciler", "device_shadow_reconciler")
        self._function("GeofenceBreachEvaluator", "geofence_breach_evaluator")

        # Direct invoke from the planning and operations services
        self._function("PredictiveMaintenanceScorer", "predictive_maintenance_scorer",
                       memory_size=1024)
        self._function("FleetCommandDispatcher", "fleet_command_dispatcher")
        self._function("TelemetryBackfillReplayer", "telemetry_backfill_replayer",
                       memory_size=1024)

        rollout = self._function("FirmwareRolloutController", "firmware_rollout_controller")
        events.Rule(
            self,
            "FirmwareRolloutSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            targets=[targets.LambdaFunction(rollout)],
        )

    def _function(
        self,
        construct_id: str,
        module: str,
        memory_size: int = 512,
    ) -> lambda_.Function:
        return lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_11,
            code=lambda_.Code.from_asset(os.path.join(LAMBDA_ROOT, "iot_telemetry")),
            handler="{0}.lambda_handler".format(module),
            memory_size=memory_size,
            timeout=Duration.seconds(30),
            reserved_concurrent_executions=10,
            dead_letter_queue=self.dlq,
            environment={"LOG_LEVEL": "INFO", "AGGREGATE_TABLE": "telemetry-window-aggregates"},
        )


app = cdk.App()
IdentityStack(app, "IdentityStack")
IotTelemetryStack(app, "IotTelemetryStack")
app.synth()
