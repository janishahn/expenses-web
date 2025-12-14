import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import get_settings
from database import session_scope
from services import RecurringRuleService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchedulerManager:
    def __init__(self) -> None:
        settings = get_settings()
        self.scheduler = BackgroundScheduler(timezone=settings.timezone)

    def _run_job(self, source: str = "manual") -> None:
        logger.info(f"scheduler_run: source={source}")
        with session_scope() as session:
            service = RecurringRuleService(session)
            count = service.catch_up_all()
            logger.info(f"scheduler_run: source={source} occurrences_posted={count}")

    def start(self) -> None:
        self._run_job("startup")

        trigger = CronTrigger(hour=3, minute=15)
        self.scheduler.add_job(
            self._run_job,
            trigger,
            args=["daily_03:15"],
            id="recurring_daily",
            replace_existing=True,
            misfire_grace_time=3600,
        )

        trigger = IntervalTrigger(hours=1)
        self.scheduler.add_job(
            self._run_job,
            trigger,
            args=["hourly_safety_net"],
            id="recurring_hourly_safety",
            replace_existing=True,
            misfire_grace_time=300,
        )

        self.scheduler.start()
        logger.info("Scheduler started with daily 03:15 and hourly safety net")

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
