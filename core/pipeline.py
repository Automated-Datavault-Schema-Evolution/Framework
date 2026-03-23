"""Top-level orchestration for one schema notification."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict

from logger import log

from config.config import DEFAULT_POLICY_NAME
from core import change_director, executor, impact_analyzer, migration_planner, policy_engine, publisher, verifier
from domain.notification_state import (
    attribute_names,
    attributes_list,
    drop_operations,
    new_link_operations,
    normalize_previous_header,
    resolve_previous_version,
    summarize_operations,
)
from handler import vault_handler_client
from helper import kafka_helper, metadata_helper



def process_notification(notification: Dict[str, Any], lake_stub: Any, vault_stub: Any, kafka_producer: Any):
    """Process one schema notification from validation through verification."""
    change_director.validate_notification(notification)
    dataset_id = notification["dataset"]["id"]
    latest = metadata_helper.load_latest_schema(dataset_id)
    previous_header, previous_header_shape = normalize_previous_header(latest)
    current_version = int(latest["version"]) if latest and latest.get("version") is not None else None

    try:
        log.info(
            f"[SEF_CORE][PIPELINE][TRACE] notification_start dataset_id={dataset_id} "
            f"correlation_id={notification.get('correlation_id')} "
            f"producer_previous_schema_version={notification.get('previous_schema_version')} "
            f"metastore_current_version={current_version} prev_header_shape={previous_header_shape}"
        )
    except Exception:
        pass

    if latest and previous_header_shape == "invalid_shape":
        try:
            header = latest.get("header")
            header_keys = sorted(list(header.keys())) if isinstance(header, dict) else str(type(header))
            log.warning(
                f"[SEF_CORE][PIPELINE][TRACE] metastore_latest_invalid_header_shape dataset_id={dataset_id} "
                f"latest_keys={sorted(list(latest.keys()))} header_keys={header_keys}"
            )
        except Exception:
            pass

    context = change_director.build_change_context(notification=notification, previous_header=previous_header)
    context["previous_version"] = resolve_previous_version(context.get("previous_version"), current_version)

    try:
        log.info(
            f"[SEF_CORE][PIPELINE][TRACE] schema_snapshot dataset_id={dataset_id} "
            f"prev_attrs={len(attributes_list(previous_header) or [])} "
            f"new_attrs={len(attributes_list(context.get('new_schema')) or [])} "
            f"prev_cols={len(attribute_names(previous_header))} new_cols={len(attribute_names(context.get('new_schema')))}"
        )
        log.info(
            f"[SEF_CORE][PIPELINE][TRACE] version_resolution dataset_id={dataset_id} "
            f"producer_previous_schema_version={notification.get('previous_schema_version')} "
            f"current_version={current_version} derived_previous_version={context.get('previous_version')}"
        )
    except Exception:
        pass

    atoms = impact_analyzer.diff_headers(prev=previous_header, new=context["new_schema"])
    try:
        atom_counts = Counter(atom.get("kind") for atom in atoms if isinstance(atom, dict))
        log.info(
            f"[SEF_CORE][PIPELINE][TRACE] diff_summary dataset_id={dataset_id} atoms_total={len(atoms) if atoms is not None else 0} atoms_by_kind={dict(atom_counts)}"
        )
    except Exception:
        pass

    impact = impact_analyzer.analyze_impact(dataset_id=dataset_id, atoms=atoms)
    metadata = notification.get("metadata", {}) or {}
    policy_name = metadata.get("policy") or DEFAULT_POLICY_NAME
    policy = policy_engine.evaluate_policies(atoms=atoms, impact=impact, policy_name=policy_name)
    policy["atoms"] = atoms
    log.info(
        f"[SEF_CORE][PIPELINE] Policy '{policy_name}' decision for dataset {dataset_id}: "
        f"{policy['decision']} (compatibility: {policy['compatibility']})"
    )

    if policy["decision"] != "allow":
        try:
            log.info(
                f"[SEF_CORE][PIPELINE] Policy denied dataset_id={dataset_id} policy={policy_name} reasons={policy.get('reasons')}"
            )
        except Exception:
            pass
        failure_event = publisher.build_failure_event(
            context=context,
            reason="policy_denied",
            details="; ".join(policy.get("reasons", [])),
        )
        kafka_helper.publish_schema_failed(kafka_producer, failure_event)
        return

    plan = migration_planner.build_plan(
        dataset_id=dataset_id,
        correlation_id=context["correlation_id"],
        atoms=atoms,
        policy=policy,
    )
    operations = list(plan.get("operations", []))
    try:
        log.info(
            f"[SEF_CORE][PIPELINE][TRACE] plan_summary dataset_id={dataset_id} plan_id={plan.get('plan_id')} "
            f"ops_total={len(operations)} ops_by_layer_kind={summarize_operations(operations)}"
        )
    except Exception:
        pass

    link_ops = new_link_operations(operations, dataset_id)
    if link_ops:
        try:
            probe = vault_handler_client.probe_link_candidates(
                vault_stub,
                correlation_id=context["correlation_id"],
                plan_id=plan["plan_id"],
                table_name=dataset_id,
            )
        except Exception as exc:
            probe = {"error_code": "DV_UNAVAILABLE", "error_message": str(exc)}

        if probe.get("error_code"):
            log.warning(
                f"[SEF_CORE][PIPELINE] Link probe failed for dataset {dataset_id} "
                f"(error_code={probe.get('error_code')}). Dropping NEW_LINK from plan to avoid a permanent error. "
                f"Details: {probe.get('error_message')}"
            )
            plan["operations"] = drop_operations(operations, link_ops)
        elif not probe.get("candidates"):
            log.info(f"[SEF_CORE][PIPELINE] No link candidates for dataset {dataset_id}. Removing NEW_LINK from plan.")
            plan["operations"] = drop_operations(operations, link_ops)
        else:
            log.info(
                f"[SEF_CORE][PIPELINE] Link probe found {len(probe['candidates']):d} candidate(s) for dataset {dataset_id}. Keeping NEW_LINK."
            )

    metadata_helper.store_plan(plan)
    execution_result = executor.execute_plan(plan, lake_stub, vault_stub)
    metadata_helper.store_execution_result(execution_result)
    try:
        log.info(
            f"[SEF_CORE][PIPELINE] Execution stored dataset_id={plan.get('dataset_id')} plan_id={plan.get('plan_id')} correlation_id={plan.get('correlation_id')}"
        )
    except Exception:
        pass

    verification = verifier.verify(plan, execution_result, lake_stub, vault_stub)
    metadata_helper.store_verification_result(verification)
    try:
        log.info(
            f"[SEF_CORE][PIPELINE] Verification outcome dataset_id={plan.get('dataset_id')} plan_id={plan.get('plan_id')} "
            f"correlation_id={plan.get('correlation_id')} status={verification.get('status')} issues={verification.get('issues')}"
        )
    except Exception:
        pass

    if verification["status"] != "passed":
        failure_event = publisher.build_failure_event(
            context=context,
            reason="verification_failed",
            details="; ".join(verification.get("issues", [])),
        )
        kafka_helper.publish_schema_failed(kafka_producer, failure_event)
        return

    new_version = metadata_helper.store_schema_version(
        dataset_id=dataset_id,
        header=context["new_schema"],
        correlation_id=context["correlation_id"],
    )
    success_event = publisher.build_success_event(
        context=context,
        new_version=new_version,
        policy=policy,
        plan=plan,
        execution=execution_result,
        verification=verification,
    )
    kafka_helper.publish_schema_evolved(kafka_producer, success_event)
