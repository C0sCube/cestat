import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Kolkata")

def scheduler_loop(logger, run_fn, sch_days: list, sch_time: list):
    """
    Runs run_fn at specified HHMM times on specified weekdays.
    All times interpreted in Asia/Kolkata timezone.
    """
    sch_time = sorted(sch_time)  # 🔒 ensure order

    while True:
        now = datetime.now(TZ)
        weekday_str = now.strftime("%a").lower()

        # ----- Skip non-run days -----
        if weekday_str not in sch_days:
            logger.info(f"Skipping today ({weekday_str.upper()}) — not in run days.")
            tomorrow = datetime.combine(
                now.date() + timedelta(days=1),
                datetime.min.time(),
                tzinfo=TZ
            )
            time.sleep((tomorrow - now).total_seconds())
            continue

        # ----- Find next run time -----
        today_times = [
            datetime.combine(
                now.date(),
                datetime.strptime(t, "%H%M").time(),
                tzinfo=TZ
            )
            for t in sch_time
        ]

        future_runs = [t for t in today_times if t > now]

        if future_runs:
            next_run = future_runs[0]
        else:
            # Move to next valid day
            next_day = now.date() + timedelta(days=1)
            while next_day.strftime("%a").lower() not in sch_days:
                next_day += timedelta(days=1)

            next_run = datetime.combine(
                next_day,
                datetime.strptime(sch_time[0], "%H%M").time(),
                tzinfo=TZ
            )

        wait_seconds = (next_run - now).total_seconds()
        logger.info(
            f"Next Scheduled Run @ {next_run.strftime('%d-%m-%Y %H:%M')} "
            f"(in {int(wait_seconds)} sec)"
        )

        time.sleep(wait_seconds)

        # ----- Execute job -----
        try:
            logger.info("=" * 60)
            logger.info(f"Running Scheduled Program @ {datetime.now(TZ).strftime('%H:%M')}")
            run_fn()
            logger.info("Completed Scheduled Run")
        except Exception as e:
            logger.critical(f"Run failed: {type(e).__name__}: {e}")