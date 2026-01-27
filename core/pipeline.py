from collections import Counter
from typing import Dict, Any, Optional, Tuple

from logger import log

from config.config import DEFAULT_POLICY_NAME
from core import change_director, impact_analyzer, policy_engine, publisher, migration_planner, executor, verifier
from handler import vault_handler_client
from helper import metadata_helper, kafka_helper


def _attributes_list(header: Any) -> Optional[list]:
    if isinstance(header, dict):
        attrs = header.get("attributes")
        if isinstance(attrs, list):
            return attrs
    return None


def _attr_names(header: Any) -> list:
    attrs = _attributes_list(header) or []
    names = []
    for a in attrs:
        if isinstance(a, dict) and a.get("name"):
            names.append(str(a.get("name")))
    return names


def _normalize_previous_header(latest: Optional[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Ensure previous_header is a dict containing an 'attributes' list.
    Parquet runs have shown metastore records where 'header' is wrapped or shaped differently.
    """
    if not latest or not isinstance(latest, dict):
        return None, "no_latest"

    # Most common: latest["header"] is already the header object
    h = latest.get("header")

    # Case 1: direct header dict with attributes list
    if isinstance(h, dict) and isinstance(h.get("attributes"), list):
        return h, "direct"

    # Case 2: header wrapper like {"header": {...}}
    if isinstance(h, dict) and isinstance(h.get("header"), dict) and isinstance(h["header"].get("attributes"), list):
        return h["header"], "wrapped_header"

    # Case 3: some stores may persist the header at the top-level (rare, but safe to support)
    if isinstance(latest.get("attributes"), list):
        # Treat latest itself as the header snapshot
        return latest, "latest_is_header"

    # Unrecognized shape
    return None, "invalid_shape"


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

    # Normalize previous header shape to avoid false "initial create" plans (parquet runs).
    previous_header, prev_header_shape = _normalize_previous_header(latest)
    current_version = int(latest["version"]) if latest and latest.get("version") is not None else None

    # High-signal trace at notification start
    try:
        log.info(
            "[SEF_CORE][TRACE] notification_start dataset_id=%s correlation_id=%s producer_previous_schema_version=%s "
            "metastore_current_version=%s prev_header_shape=%s",
            dataset_id,
            notification.get("correlation_id"),
            notification.get("previous_schema_version"),
            current_version,
            prev_header_shape,
        )
    except Exception:
        pass

    if latest and prev_header_shape == "invalid_shape":
        try:
            log.warning(
                "[SEF_CORE][TRACE] metastore_latest_invalid_header_shape dataset_id=%s latest_keys=%s header_keys=%s",
                dataset_id,
                sorted(list(latest.keys())),
                sorted(list(latest.get("header", {}).keys())) if isinstance(latest.get("header"), dict) else str(
                    type(latest.get("header"))),
            )
        except Exception:
            pass

    context = change_director.build_change_context(
        notification=notification,
        previous_header=previous_header,
    )

    # CRITICAL FIX:
    # FileWatcher sets previous_schema_version=null. Tests (and correct semantics) expect
    # previous_version to reflect the metastore version on drift events.
    if context.get("previous_version") is None and current_version is not None:
        context["previous_version"] = current_version

    # Log schema sizes to make diff failures obvious
    try:
        log.info(
            "[SEF_CORE][TRACE] schema_snapshot dataset_id=%s prev_attrs=%s new_attrs=%s prev_cols=%s new_cols=%s",
            dataset_id,
            len(_attributes_list(previous_header) or []),
            len(_attributes_list(context.get("new_schema")) or []),
            len(_attr_names(previous_header)),
            len(_attr_names(context.get("new_schema"))),
        )
    except Exception:
        pass

    # Keep trace logging granular; add the derived value to make future failures obvious.
    try:
        log.info(
            "[SEF_CORE][TRACE] version_resolution dataset_id=%s producer_previous_schema_version=%s current_version=%s derived_previous_version=%s",
            dataset_id,
            notification.get("previous_schema_version"),
            current_version,
            context.get("previous_version"),
        )
    except Exception:
        pass

    # 2) Diff + impact
    atoms = impact_analyzer.diff_headers(
        prev=previous_header,
        new=context["new_schema"],
    )

    # Summarize atoms (kind counts) for diagnostics
    try:
        kind_counts = Counter([a.get("kind") for a in atoms if isinstance(a, dict)])
        log.info(
            "[SEF_CORE][TRACE] diff_summary dataset_id=%s atoms_total=%s atoms_by_kind=%s",
            dataset_id,
            len(atoms) if atoms is not None else 0,
            dict(kind_counts),
        )
    except Exception:
        pass

    impact = impact_analyzer.analyze_impact(
        dataset_id=dataset_id,
        atoms=atoms,
    )

    # 3) Policy evaluation (multi-policy based on metadata / env)
    metadata = notification.get("metadata", {}) or {}
    policy_name = metadata.get("policy") or DEFAULT_POLICY_NAME

    policy = policy_engine.evaluate_policies(
        atoms=atoms,
        impact=impact,
        policy_name=policy_name,
    )
    policy["atoms"] = atoms

    log.info(
        "[SEF_CORE][PIPELINE] Policy '%s' decision for dataset %s: %s (compatibility: %s)",
        policy_name,
        dataset_id,
        policy["decision"],
        policy["compatibility"],
    )

    if policy["decision"] != "allow":
        # Provide reasons in log to reduce guesswork
        try:
            log.info(
                "[SEF_CORE][PIPELINE] Policy denied dataset_id=%s policy=%s reasons=%s",
                dataset_id,
                policy_name,
                policy.get("reasons"),
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

    # 4) Plan construction
    plan = migration_planner.build_plan(
        dataset_id=dataset_id,
        correlation_id=context["correlation_id"],
        atoms=atoms,
        policy=policy,
    )

    # Plan summary (counts by layer/kind)
    try:
        ops = list(plan.get("operations", []))
        op_counts = Counter([(op.get("layer"), op.get("kind")) for op in ops if isinstance(op, dict)])
        log.info(
            "[SEF_CORE][TRACE] plan_summary dataset_id=%s plan_id=%s ops_total=%s ops_by_layer_kind=%s",
            dataset_id,
            plan.get("plan_id"),
            len(ops),
            {f"{k[0]}::{k[1]}": v for k, v in op_counts.items()},
        )
    except Exception:
        ops = list(plan.get("operations", []))

    # Only keep NEW_LINK if DV reports candidates exist.
    new_link_ops = [
        op
        for op in ops
        if op.get("layer") == "vault" and op.get("kind") == "NEW_LINK" and op.get("target") == dataset_id
    ]
    if new_link_ops:
        try:
            probe = vault_handler_client.probe_link_candidates(
                vault_stub,
                correlation_id=context["correlation_id"],
                plan_id=plan["plan_id"],
                table_name=dataset_id,
            )
        except Exception as e:
            # Treat DV unavailability as a probe failure and drop NEW_LINK to avoid aborting
            # the whole notification. The actual vault operations will still be executed (and
            # fail) if the handler is truly down.
            probe = {"error_code": "DV_UNAVAILABLE", "error_message": str(e)}

        if probe.get("error_code"):
            log.warning(
                "[SEF_CORE][PIPELINE] Link probe failed for dataset %s (error_code=%s). "
                "Dropping NEW_LINK from plan to avoid a permanent error. Details: %s",
                dataset_id,
                probe.get("error_code"),
                probe.get("error_message"),
            )
            plan["operations"] = [op for op in ops if op not in new_link_ops]
        else:
            if not probe.get("candidates"):
                log.info(
                    "[SEF_CORE][PIPELINE] No link candidates for dataset %s. Removing NEW_LINK from plan.",
                    dataset_id,
                )
                plan["operations"] = [op for op in ops if op not in new_link_ops]
            else:
                log.info(
                    "[SEF_CORE][PIPELINE] Link probe found %d candidate(s) for dataset %s. Keeping NEW_LINK.",
                    len(probe["candidates"]),
                    dataset_id,
                )

    metadata_helper.store_plan(plan)

    # Execution via gRPC handlers
    execution_result = executor.execute_plan(
        plan,
        lake_stub,
        vault_stub,
    )
    metadata_helper.store_execution_result(execution_result)

    # Execution summary log
    try:
        log.info(
            "[SEF_CORE][PIPELINE] Execution stored dataset_id=%s plan_id=%s correlation_id=%s",
            plan.get("dataset_id"),
            plan.get("plan_id"),
            plan.get("correlation_id"),
        )
    except Exception:
        pass

    # 5) Verification step
    verification = verifier.verify(
        plan,
        execution_result,
        lake_stub,
        vault_stub,
    )
    metadata_helper.store_verification_result(verification)

    # Guard log (keep)
    try:
        log.info(
            "[SEF_CORE][PIPELINE] Verification outcome dataset_id=%s plan_id=%s correlation_id=%s status=%s issues=%s",
            plan.get("dataset_id"),
            plan.get("plan_id"),
            plan.get("correlation_id"),
            verification.get("status"),
            verification.get("issues"),
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

    # 6) Store new schema version & publish success event
    new_version = metadata_helper.store_schema_version(
        dataset_id=dataset_id,
        header=context["new_schema"],
        correlation_id=context["correlation_id"],
    )

    # Hard guard: parquet runs have shown new_version=None in logs; never emit that.
    if not isinstance(new_version, int):
        fallback_version = int((context.get("previous_version") or 0) + 1)
        try:
            log.warning(
                "[SEF_CORE][PIPELINE] store_schema_version returned non-int (value=%s). Using fallback=%s "
                "(dataset_id=%s correlation_id=%s)",
                new_version,
                fallback_version,
                dataset_id,
                context.get("correlation_id"),
            )
        except Exception:
            pass
        new_version = fallback_version
    else:
        try:
            log.info(
                "[SEF_CORE][TRACE] store_schema_version_ok dataset_id=%s correlation_id=%s new_version=%s",
                dataset_id,
                context.get("correlation_id"),
                new_version,
            )
        except Exception:
            pass

    event = publisher.build_schema_evolved_event(
        context=context,
        policy=policy,
        plan=plan,
        execution=execution_result,
        verification=verification,
        new_version=new_version,
    )
    kafka_helper.publish_schema_evolved(kafka_producer, event)
