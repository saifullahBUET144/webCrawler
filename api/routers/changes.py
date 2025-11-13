import logging
import pandas as pd
from io import StringIO
from enum import Enum
from fastapi import APIRouter, Query, Depends, Request, HTTPException, status
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from starlette.responses import StreamingResponse
from datetime import datetime, timedelta
from crawler.parser import ChangeLogEntry
from ..security import get_api_key

log = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(get_api_key)])

class ReportFormat(str, Enum):
    json = "json"
    csv = "csv"

@router.get("/", response_model=List[ChangeLogEntry])
async def get_recent_changes(
    request: Request,
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get a list of the most recent changes detected by the scheduler.
    """
    db: AsyncIOMotorDatabase = request.app.state.db
    cursor = db.change_log.find().sort("timestamp", -1).limit(limit)
    return [ChangeLogEntry(**doc) async for doc in cursor]

@router.get("/report")
async def get_daily_change_report(
    request: Request,
    format: ReportFormat = Query(ReportFormat.json, description="Output format")
):
    """
    Get a report of all changes detected in the last 24 hours.
    This fulfills the Part 2 requirement.
    """
    db: AsyncIOMotorDatabase = request.app.state.db
    
    naive_now_utc = datetime.utcnow()
    
    # Calculate the cutoff time - compare datetime objects, not strings
    cutoff_time_dt = naive_now_utc - timedelta(days=1)
    query = {"timestamp": {"$gte": cutoff_time_dt}}  # Remove .isoformat() conversion
    
    cursor = db.change_log.find(query).sort("timestamp", -1)
    changes = [ChangeLogEntry(**doc) async for doc in cursor]
    
    if format == ReportFormat.json:
        return changes
        
    if format == ReportFormat.csv:
        if not changes:
            return StreamingResponse(
                content="No changes found in the last 24 hours.",
                media_type="text/csv"
            )
        
        changes_data = [c.model_dump() for c in changes]
        df = pd.DataFrame(changes_data)
        
        df = df.drop(columns=["id"], errors="ignore")
        
        stream = StringIO()
        df.to_csv(stream, index=False)
        
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = \
            "attachment; filename=daily_change_report.csv"
        return response