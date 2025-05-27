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

    # conn.execute(text(
    #     """
    #     CREATE TABLE IF NOT EXISTS freelancer(
    #         id INTEGER PRIMARY KEY,
    #         job_category TEXT,
    #         platform TEXT,
    #         experience_level TEXT,
    #         client_region TEXT,
    #         payment_method TEXT,
    #         job_completed INTEGER,
    #         earnings_usd INTEGER,
    #         hourly_rate REAL,
    #         job_success_rate REAL,
    #         client_rating REAL,
    #         job_duration_days INTEGER,
    #         project_type TEXT,
    #         rehire_rate REAL,
    #         marketing_spend INTEGER
    #     )
    #     """
    # ))
    # conn.execute(
    #     "INSERT INTO freelancer VALUES (%s)"
    # )
    # for row in reader:
    #     quests = ", ".join("?" for _ in row)
    #     cur.execute(
    #         "INSERT INTO freelancer VALUES (%s)" % quests, (
    #             int(row["Freelancer_ID"]),
    #             row["Job_Category"],
    #             row["Platform"],
    #             row["Experience_Level"],
    #             row["Client_Region"],
    #             row["Payment_Method"],
    #             int(row["Job_Completed"]),
    #             int(row["Earnings_USD"]),
    #             float(row["Hourly_Rate"]),
    #             float(row["Job_Success_Rate"]),
    #             float(row["Client_Rating"]),
    #             int(row["Job_Duration_Days"]),
    #             row["Project_Type"],
    #             float(row["Rehire_Rate"]),
    #             int(row["Marketing_Spend"])
    #         )
    #     )
    # con.commit()