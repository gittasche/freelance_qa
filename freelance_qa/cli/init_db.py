import click
import csv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from freelance_qa.config import get_config
from freelance_qa.models import Freelancer, Base


@click.command()
def init_db():
    db_engine = create_engine(get_config().DB_DSN)
    Base.metadata.create_all(db_engine)
    reader = csv.DictReader(open("freelancer_earnings_bd.csv", newline="", encoding="utf-8"))
    with Session(db_engine) as sess, sess.begin():
        for row in reader:
            sess.add(Freelancer(
                id=int(row["Freelancer_ID"]),
                job_category=row["Job_Category"],
                platform=row["Platform"],
                experience_level=row["Experience_Level"],
                client_region=row["Client_Region"],
                payment_method=row["Payment_Method"],
                job_completed=int(row["Job_Completed"]),
                earnings_usd=int(row["Earnings_USD"]),
                hourly_rate=float(row["Hourly_Rate"]),
                job_success_rate=float(row["Job_Success_Rate"]),
                client_rating=float(row["Client_Rating"]),
                job_duration_days=int(row["Job_Duration_Days"]),
                project_type=row["Project_Type"],
                rehire_rate=float(row["Rehire_Rate"]),
                marketing_spend=int(row["Marketing_Spend"])
            ))
