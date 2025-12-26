from typing import Dict, Any

from logger import log

from core import change_director, impact_analyzer, policy_engine, publisher, migration_planner, executor, verifier
from helper import metadata_helper, kafka_helper


def process_notification(
        notification: Dict[str, Any],
        lake_stub: Any,
        vault_stub: Any,
        kafka_producer: Any,
):
    """
    End-to-end processing for a single schema.notification.

    Steps:
      1) Validate and build change context
      2) Diff headers and analyze impact
      3) Evaluate policy (production vs sandbox etc.)
      4) If allowed: build plan, execute via gRPC handlers
      5) Verification step:
           - check execution results
           - introspect lake/vault for structural evidence
           - persist verification result
           - abort with failure event if verification failed
      6) Persist new schema version and publish schema.evolved event
    """
    # 1) Admission / normalization
    change_director.validate_notification(notification)
    dataset_id = notification["dataset"]["id"]

    latest = metadata_helper.load_latest_schema(dataset_id)
    previous_header = latest["header"] if latest else None

    context = change_director.build_change_context(
        notification=notification,
        previous_header=previous_header,
    )

    # 2) Diff + impact
    atoms = impact_analyzer.diff_headers(
        prev=previous_header,
        new=context["new_schema"],
    )
    impact = impact_analyzer.analyze_impact(
        dataset_id=dataset_id,
        atoms=atoms,
    )

    # 3) Policy evaluation (multi-policy based on metadata / env)
    metadata = notification.get("metadata", {}) or {}
    policy_name = metadata.get("policy") or cfg.DEFAULT_POLICY_NAME

    policy = policy_engine.evaluate_policies(
        atoms=atoms,
        impact=impact,
        policy_name=policy_name,
    )
    # Optionally attach atoms for publisher/introspection
    policy["atoms"] = atoms

    log.info(
        "[SEF_CORE][PIPELINE] Policy '%s' decision for dataset %s: %s "
        "(compatibility: %s)",
        policy_name,
        dataset_id,
        policy["decision"],
        policy["compatibility"],
    )

    if policy["decision"] != "allow":
        failure_event = publisher.build_failure_event(
            context=context,
            reason="policy_denied",
            details="; ".join(policy.get("reasons", [])),
        )
        kafka_helper.publish_schema_evolved(kafka_producer, failure_event)
        return

    # 4) Plan construction
    plan = migration_planner.build_plan(
        dataset_id=dataset_id,
        correlation_id=context["correlation_id"],
        atoms=atoms,
        policy=policy,
    )
    metadata_helper.store_plan(plan)

    # Execution via gRPC handlers
    execution_result = executor.execute_plan(
        plan,
        lake_stub,
        vault_stub,
    )
    metadata_helper.store_execution_result(execution_result)

    # 5) Verification step
    verification = verifier.verify(
        plan,
        execution_result,
        lake_stub,
        vault_stub,
    )
    metadata_helper.store_verification_result(verification)

    if verification["status"] != "passed":
        # Use 'issues' from VerificationResult (core.model) as the error details
        failure_event = publisher.build_failure_event(
            context=context,
            reason="verification_failed",
            details="; ".join(verification.get("issues", [])),
        )
        kafka_helper.publish_schema_evolved(kafka_producer, failure_event)
        return

    # 6) Store new schema version & publish success event
    new_version = metadata_helper.store_schema_version(
        dataset_id=dataset_id,
        header=context["new_schema"],
        correlation_id=context["correlation_id"],
    )
    event = publisher.build_schema_evolved_event(
        context=context,
        policy=policy,
        plan=plan,
        execution=execution_result,
        verification=verification,
        new_version=new_version,
    )
    kafka_helper.publish_schema_evolved(kafka_producer, event)
