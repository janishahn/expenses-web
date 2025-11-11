from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import get_settings
from database import session_scope
from services import RecurringRuleService


class SchedulerManager:
    def __init__(self) -> None:
        settings = get_settings()
        self.scheduler = BackgroundScheduler(timezone=settings.timezone)

    def _run_job(self) -> None:
        with session_scope() as session:
            service = RecurringRuleService(session)
            service.catch_up_all()

    def start(self) -> None:
        self._run_job()
        trigger = IntervalTrigger(hours=1)
        self.scheduler.add_job(self._run_job, trigger, id="recurring_catch_up", replace_existing=True)
        self.scheduler.start()

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
