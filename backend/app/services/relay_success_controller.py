from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.base import SessionLocal
from app.models.acquisition_supervisor import AcquisitionEvent, AcquisitionProspect
from app.models.relay_intent import RelayIntentEvent, RelayIntentLead
from app.services.custom_outreach import outreach_status
from app.services.post_purchase_autopilot import (
    run_inbound_conversion_sweep,
    run_paid_intake_reminder_sweep,
    run_post_delivery_upsell_sweep,
)


SUCCESS_TICK_EVENT = "relay_success_control_tick"


def _session() -> Session:
    return SessionLocal()


def _now() -> datetime:
    return datetime.utcnow()


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _event_count(session: Session, event_type: str, *, since: datetime, like: bool = False) -> int:
    stmt = select(func.count(AcquisitionEvent.id))
    stmt = stmt.where(AcquisitionEvent.event_type.like(event_type) if like else AcquisitionEvent.event_type == event_type)
    stmt = stmt.where(AcquisitionEvent.created_at >= since)
    return int(session.execute(stmt).scalar() or 0)


def _intent_count(session: Session, event_type: str, *, since: datetime) -> int:
    return int(
        session.execute(
            select(func.count(RelayIntentEvent.id))
            .where(RelayIntentEvent.event_type == event_type)
            .where(RelayIntentEvent.created_at >= since)
        ).scalar()
        or 0
    )


def _lead_count(session: Session, source_term: str | None, *, since: datetime) -> int:
    stmt = select(func.count(RelayIntentLead.id)).where(RelayIntentLead.created_at >= since)
    if source_term:
        stmt = stmt.where(RelayIntentLead.source.ilike(f"%{source_term}%"))
    return int(session.execute(stmt).scalar() or 0)


def _stripe_email(payload: dict[str, Any]) -> str:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    return str(
        payload.get("customer_details", {}).get("email")
        or payload.get("customer_email")
        or payload.get("email")
        or raw_object.get("customer_details", {}).get("email")
        or raw_object.get("customer_email")
        or ""
    ).strip().lower()


def _stripe_amount_cents(payload: dict[str, Any]) -> int:
    raw_object = payload.get("raw", {}).get("data", {}).get("object", {})
    try:
        return int(payload.get("amount_total") or raw_object.get("amount_total") or 0)
    except Exception:
        return 0


def _paid_for_email(session: Session, email: str) -> bool:
    email = (email or "").strip().lower()
    if not email:
        return False
    prospect = session.execute(
        select(AcquisitionProspect)
        .where(AcquisitionProspect.contact_email == email)
        .where(AcquisitionProspect.stripe_status == "paid")
        .limit(1)
    ).scalar_one_or_none()
    if prospect is not None:
        return True

    events = session.execute(
        select(AcquisitionEvent.payload_json)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .order_by(AcquisitionEvent.created_at.desc())
        .limit(100)
    ).scalars().all()
    for raw in events:
        if email in json.dumps(_safe_json(raw), ensure_ascii=False).lower():
            return True
    return False


def _money_metrics(session: Session, *, since: datetime) -> dict[str, Any]:
    events = session.execute(
        select(AcquisitionEvent)
        .where(AcquisitionEvent.event_type == "stripe_paid")
        .where(AcquisitionEvent.created_at >= since)
        .order_by(AcquisitionEvent.created_at.desc())
    ).scalars().all()

    payments = 0
    gross_cents = 0
    for event in events:
        payload = _safe_json(event.payload_json)
        if _stripe_email(payload) == "pham.alann@gmail.com":
            continue
        payments += 1
        gross_cents += _stripe_amount_cents(payload)

    return {
        "payments": payments,
        "gross_cents": gross_cents,
        "gross_usd": round(gross_cents / 100.0, 2),
    }


def _due_followup_counts(session: Session, *, now: datetime) -> dict[str, int]:
    messy_cutoff = now - timedelta(hours=int(os.getenv("RELAY_MESSY_NOTES_FOLLOWUP_HOURS", "2") or "2"))
    sample_cutoff = now - timedelta(hours=int(os.getenv("RELAY_SAMPLE_FOLLOWUP_HOURS", "24") or "24"))

    messy_due = 0
    sample_due = 0

    messy_leads = session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.source.ilike("%messy_notes%"))
        .where(RelayIntentLead.created_at <= messy_cutoff)
        .limit(100)
    ).scalars().all()
    for lead in messy_leads:
        if _paid_for_email(session, lead.email):
            continue
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead.id}")
            .where(AcquisitionEvent.event_type == "autopilot_messy_notes_checkout_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is None:
            messy_due += 1

    sample_leads = session.execute(
        select(RelayIntentLead)
        .where(RelayIntentLead.source.ilike("%sample%"))
        .where(RelayIntentLead.created_at <= sample_cutoff)
        .limit(100)
    ).scalars().all()
    for lead in sample_leads:
        if _paid_for_email(session, lead.email):
            continue
        exists = session.execute(
            select(AcquisitionEvent.id)
            .where(AcquisitionEvent.prospect_external_id == f"relay-lead:{lead.id}")
            .where(AcquisitionEvent.event_type == "autopilot_sample_notes_followup_sent")
            .limit(1)
        ).scalar_one_or_none()
        if exists is None:
            sample_due += 1

    return {"messy_notes_due": messy_due, "sample_request_due": sample_due}


def _env_snapshot() -> dict[str, bool]:
    return {
        "DATABASE_URL": bool(settings.database_url),
        "RESEND_API_KEY": bool(settings.resend_api_key),
        "PACKET_CHECKOUT_URL": bool(settings.packet_checkout_url),
        "CLIENT_INTAKE_DESTINATION": bool(settings.client_intake_destination or os.getenv("CLIENT_INTAKE_URL", "").strip()),
        "FROM_EMAIL_FULFILLMENT": bool(settings.from_email_fulfillment),
        "APOLLO_API_KEY": bool(settings.apollo_api_key),
        "BUYER_MAILBOX_PASSWORD": bool(settings.buyer_acq_mailbox_password),
    }


def relay_success_snapshot(days: int = 7) -> dict[str, Any]:
    days = max(1, min(int(days), 90))
    now = _now()
    since = now - timedelta(days=days)
    with _session() as session:
        outreach = outreach_status()
        money = _money_metrics(session, since=since)
        page_views = _intent_count(session, "page_view", since=since)
        checkout_clicks = _intent_count(session, "checkout_click", since=since)
        notes_clicks = _intent_count(session, "note_intake_click", since=since)
        lead_count = _lead_count(session, None, since=since)
        messy_notes = _lead_count(session, "messy_notes", since=since)
        sample_requests = _lead_count(session, "sample", since=since)
        sends = _event_count(session, "custom_outreach_sent_step_%", since=since, like=True)
        replies = (
            _event_count(session, "custom_outreach_reply_seen", since=since)
            + _event_count(session, "smartlead_reply", since=since)
        )
        fulfilled = _event_count(session, "autopilot_paid_relay_notes_fulfilled", since=since)
        onboarding = _event_count(session, "autopilot_paid_onboarding_sent", since=since)
        inbound_followups = (
            _event_count(session, "autopilot_messy_notes_checkout_followup_sent", since=since)
            + _event_count(session, "autopilot_sample_notes_followup_sent", since=since)
        )
        due_followups = _due_followup_counts(session, now=now)

    env = _env_snapshot()
    critical_missing = [
        name
        for name in ["DATABASE_URL", "RESEND_API_KEY", "PACKET_CHECKOUT_URL", "FROM_EMAIL_FULFILLMENT"]
        if not env.get(name)
    ]

    return {
        "status": "ok",
        "days": days,
        "since": since.isoformat(),
        "env": env,
        "critical_missing": critical_missing,
        "money": money,
        "intent": {
            "page_views": page_views,
            "notes_clicks": notes_clicks,
            "checkout_clicks": checkout_clicks,
            "lead_count": lead_count,
            "messy_notes": messy_notes,
            "sample_requests": sample_requests,
        },
        "outreach": {
            "sends": sends,
            "replies": replies,
            "reply_rate": round(replies / sends, 4) if sends else 0,
            "due_now": int(outreach.get("due_now_count") or outreach.get("queued_count") or 0),
            "sent_today": int(outreach.get("sent_today") or 0),
            "daily_send_cap": int(outreach.get("daily_send_cap") or 0),
            "cap_remaining": int(outreach.get("cap_remaining") or 0),
            "next_money_move": outreach.get("next_money_move", ""),
        },
        "conversion": {
            "inbound_followups_sent": inbound_followups,
            "messy_notes_followups_due": due_followups["messy_notes_due"],
            "sample_followups_due": due_followups["sample_request_due"],
            "paid_onboarding_sent": onboarding,
            "paid_notes_fulfilled": fulfilled,
        },
    }


def _bottleneck(snapshot: dict[str, Any]) -> str:
    if snapshot.get("critical_missing"):
        return "infrastructure_blocked"

    money = snapshot["money"]
    intent = snapshot["intent"]
    outreach = snapshot["outreach"]
    conversion = snapshot["conversion"]

    if int(money.get("payments") or 0) > 0 and int(conversion.get("paid_notes_fulfilled") or 0) < int(money.get("payments") or 0):
        return "paid_fulfillment"
    if int(conversion.get("messy_notes_followups_due") or 0) > 0:
        return "messy_notes_to_payment"
    if int(conversion.get("sample_followups_due") or 0) > 0:
        return "sample_to_notes"
    if int(intent.get("checkout_clicks") or 0) > int(money.get("payments") or 0):
        return "checkout_to_payment"
    if int(intent.get("lead_count") or 0) == 0 and int(intent.get("page_views") or 0) >= 20:
        return "page_to_lead"
    if int(intent.get("page_views") or 0) < 20 and int(outreach.get("sends") or 0) < 20:
        return "traffic"
    if int(outreach.get("sends") or 0) >= 30 and int(outreach.get("replies") or 0) == 0:
        return "outbound_targeting_or_copy"
    if int(outreach.get("due_now") or 0) == 0 and int(outreach.get("cap_remaining") or 0) > 0:
        return "lead_refill"
    return "running"


def _next_action(bottleneck: str) -> str:
    actions = {
        "infrastructure_blocked": "Fix missing production credentials before trying to scale.",
        "paid_fulfillment": "Fulfill paid buyers and keep reminders active until delivery is complete.",
        "messy_notes_to_payment": "Send the notes-to-checkout follow-up.",
        "sample_to_notes": "Send the sample-to-notes follow-up.",
        "checkout_to_payment": "Keep notes-first friction low and make the paid test obvious after interest.",
        "page_to_lead": "Improve the first-screen ask before changing the backend.",
        "traffic": "Let direct-buyer outbound refill and send; the system needs more qualified traffic.",
        "outbound_targeting_or_copy": "Do not scale volume; rotate one controlled experiment and target direct buyers only.",
        "lead_refill": "Refill direct decision-maker leads.",
        "running": "Keep the loop steady and avoid random changes.",
    }
    return actions.get(bottleneck, actions["running"])


def run_relay_success_control_tick() -> dict[str, Any]:
    before = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(before)

    actions: dict[str, Any] = {}
    actions["inbound_conversion"] = run_inbound_conversion_sweep()
    actions["paid_intake_reminders"] = run_paid_intake_reminder_sweep(
        hours=int(os.getenv("OPS_INTAKE_REMINDER_HOURS", "12") or "12")
    )
    actions["post_delivery_upsell"] = run_post_delivery_upsell_sweep(
        hours=int(os.getenv("OPS_UPSELL_DELAY_HOURS", "24") or "24")
    )

    after = relay_success_snapshot(days=7)
    result = {
        "status": "ok",
        "bottleneck": bottleneck,
        "next_action": _next_action(bottleneck),
        "before": before,
        "actions": actions,
        "after": after,
        "created_at": _now().isoformat(),
    }

    with _session() as session:
        session.add(
            AcquisitionEvent(
                event_type=SUCCESS_TICK_EVENT,
                prospect_external_id="relay-success",
                summary=f"{bottleneck}: {_next_action(bottleneck)}",
                payload_json=json.dumps(result, ensure_ascii=False),
            )
        )
        session.commit()

    return result


def relay_success_status() -> dict[str, Any]:
    snapshot = relay_success_snapshot(days=7)
    bottleneck = _bottleneck(snapshot)
    with _session() as session:
        latest = session.execute(
            select(AcquisitionEvent)
            .where(AcquisitionEvent.event_type == SUCCESS_TICK_EVENT)
            .order_by(AcquisitionEvent.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    return {
        "status": "ok",
        "bottleneck": bottleneck,
        "next_action": _next_action(bottleneck),
        "snapshot": snapshot,
        "latest_tick": {
            "created_at": latest.created_at.isoformat(),
            "summary": latest.summary,
            "payload": _safe_json(latest.payload_json),
        }
        if latest
        else None,
    }
