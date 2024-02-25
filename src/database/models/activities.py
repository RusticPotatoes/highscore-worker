from database.database import Base
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.schema import UniqueConstraint


# CREATE TABLE activities (
#   activity_id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, # < 255
#   activity_name VARCHAR(50) NOT NULL,
#   UNIQUE KEY unique_activity_name (activity_name)
# );
class Activities(Base):
    __tablename__ = "activities"

    activity_id = Column(TINYINT, primary_key=True, autoincrement=True)
    activity_name = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint("activity_name", name="unique_activity_name"),)


# CREATE TABLE player_activities (
#   scraper_id BIGINT UNSIGNED NOT NULL,
#   activity_id TINYINT UNSIGNED NOT NULL,
#   activity_value INT UNSIGNED NOT NULL DEFAULT 0, # some guy could get over 65k kc
#   FOREIGN KEY (scraper_id) REFERENCES scraper_data(scraper_id) ON DELETE CASCADE,
#   FOREIGN KEY (activity_id) REFERENCES activities(activity_id) ON DELETE CASCADE,
#   PRIMARY KEY (scraper_id, activity_id)
# );


class PlayerActivities(Base):
    __tablename__ = "player_activities"

    scraper_id = Column(
        BIGINT,
        ForeignKey("scraper_data.scraper_id", ondelete="CASCADE"),
        primary_key=True,
    )
    activity_id = Column(
        TINYINT,
        ForeignKey("activities.activity_id", ondelete="CASCADE"),
        primary_key=True,
    )
    activity_value = Column(Integer, nullable=False, default=0)
