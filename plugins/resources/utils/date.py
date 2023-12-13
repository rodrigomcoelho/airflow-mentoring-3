from pendulum import timezone
from datetime import datetime, timedelta

TIMEZONE = timezone("America/Sao_Paulo")


def days_ago(n: int) -> datetime:
    date_now = datetime.now(tz=TIMEZONE)
    new_date = datetime(
        year=date_now.year,
        month=date_now.month,
        day=date_now.day,
        tzinfo=TIMEZONE,
    )

    new_date -= timedelta(days=n)

    return new_date
