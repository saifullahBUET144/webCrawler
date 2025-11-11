import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .tasks import run_daily_change_detection
from utils.logging import setup_logging

log = logging.getLogger(__name__)

async def main():
    log.info("Starting scheduler service...")
    scheduler = AsyncIOScheduler()
    
    # Schedule the job to run every day at 3:00 AM
    scheduler.add_job(
        run_daily_change_detection,
        trigger=CronTrigger(hour=3, minute=0),
        name="Daily Book Change Detection"
    )
    
    ### For Testing
    # Uncomment the following line to test the job immediately
    #scheduler.add_job(run_daily_change_detection, "date")
    
    scheduler.start()
    
    # Keep the script alive
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler service shut down.")
        scheduler.shutdown()

if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())