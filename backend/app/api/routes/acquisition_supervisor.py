from __future__ import annotations

import asyncio

from fastapi import APIRouter, BackgroundTasks, Request

from app.services.acquisition_supervisor import (
    acquisition_digest,
    handle_intake_webhook,
    handle_smartlead_reply_webhook,
    handle_stripe_purchase_webhook,
    import_from_apollo_people_search,
    import_from_apollo_search,
    tick_supervisor,
)

router = APIRouter()


@router.get("/digest")
async def supervisor_digest() -> dict:
    return acquisition_digest()


@router.post("/apollo-search")
def apollo_search(body: dict, background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(run_apollo_search, body)
    return {"status": "accepted"}


@router.post("/apollo-people-search")
def apollo_people_search(body: dict, background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(run_apollo_people_search, body)
    return {"status": "accepted"}


@router.post("/tick")
def tick(body: dict, background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(run_tick, body)
    return {"status": "accepted"}


@router.post("/webhooks/smartlead")
async def supervisor_smartlead_webhook(request: Request) -> dict:
    payload = await request.json()
    return await handle_smartlead_reply_webhook(payload)


@router.post("/webhooks/stripe")
async def supervisor_stripe_webhook(request: Request) -> dict:
    payload = await request.json()
    return handle_stripe_purchase_webhook(payload)


@router.post("/webhooks/intake")
async def supervisor_intake_webhook(request: Request) -> dict:
    payload = await request.json()
    return handle_intake_webhook(payload)


def run_apollo_search(body: dict) -> None:
    try:
        asyncio.run(import_from_apollo_search(body))
    except Exception as e:
        print("apollo_search error:", e)


def run_apollo_people_search(body: dict) -> None:
    try:
        asyncio.run(import_from_apollo_people_search(body))
    except Exception as e:
        print("apollo_people_search error:", e)


def run_tick(body: dict) -> None:
    try:
        send_live = body.get("send_live", True)
        asyncio.run(tick_supervisor(send_live=send_live))
    except Exception as e:
        print("tick error:", e)
