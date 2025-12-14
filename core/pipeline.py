from typing import Dict, Any

from logger import log

from core import change_director, impact_analyzer, policy_engine, publisher, migration_planner, executor, verifier
from helper import metadata_helper, kafka_helper


def process_notification(notification: Dict[str, Any], lake_stub: Any, vault_stub: Any, kafka_producer: Any):
    """
    End-to-end processing for a single schema.notification
    """
    # Admission / normalization
    change_director.validate_notification(notification)
    dataset_id = notification["dataset"]["id"]

    latest = metadata_helper.load_latest_schema(dataset_id)
    previous_header = latest["header"] if latest else None

    context = change_director.build_change_context(notification=notification, previous_header=previous_header)

    # Diff + impact
    atoms = impact_analyzer.diff_headers(prev=previous_header, new=context["new_schema"])
    impact = impact_analyzer.analyze_impact(dataset_id=dataset_id, atoms=atoms)

    # Policy evaluation
    policy = policy_engine.evaluate_policies(atoms=atoms, impact=impact)
    log.info(
        f"[SEF_CORE][PIPELINE] Policy decision for dataset {dataset_id}: {policy["decision"]} (compatibility: {policy['compatibility']})")

    if policy["decision"] != "allow":
        failure_event = publisher.build_failure_event(
            context=context,
            reason="blocked_by_policy",
            details="; ".join(policy.get("reasons", []))
        )
        kafka_helper.publish_schema_evolved(kafka_producer, failure_event)
        return

    # Plan construction
    plan = migration_planner.build_plan(
        dataset_id=dataset_id,
        correlation_id=context["correlation_id"],
        atoms=atoms,
        policy=policy,
    )
    metadata_helper.store_plan(plan)

    # Execution
    execution_result = executor.execute_plan(plan, lake_stub, vault_stub)
    metadata_helper.store_execution_result(execution_result)

    # Verification
    verification = verifier.verify(plan, execution_result, lake_stub, vault_stub)
    metadata_helper.store_verification_result(verification)
    if verification["status"] != "passed":
        failure_event = publisher.build_failure_event(
            context=context,
            reason="verification_failed",
            details="; ".join(verification.get("reasons", [])),
        )
        kafka_helper.publish_schema_evolved(kafka_producer, failure_event)
        return

    # Store new schema version & publish
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
