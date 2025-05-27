from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.declarative import declared_attr


class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class IdMixin:
    id: Mapped[int] = mapped_column(primary_key=True)


class Freelancer(Base, IdMixin):
    job_category: Mapped[str]
    platform: Mapped[str]
    experience_level: Mapped[str]
    client_region: Mapped[str]
    payment_method: Mapped[str]
    job_completed: Mapped[int]
    earnings_usd: Mapped[int]
    hourly_rate: Mapped[float]
    job_success_rate: Mapped[float]
    client_rating: Mapped[float]
    job_duration_days: Mapped[int]
    project_type: Mapped[str]
    rehire_rate: Mapped[float]
    marketing_spend: Mapped[int]