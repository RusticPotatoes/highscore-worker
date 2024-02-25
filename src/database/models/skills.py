from database.database import Base
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.schema import UniqueConstraint


# CREATE TABLE skills (
#   skill_id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, # < 255
#   skill_name VARCHAR(50) NOT NULL,
#   UNIQUE KEY unique_skill_name (skill_name)
# );
class Skills(Base):
    __tablename__ = "skills"

    skill_id = Column(TINYINT, primary_key=True, autoincrement=True)
    skill_name = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint("skill_name", name="unique_skill_name"),)


# CREATE TABLE player_skills (
#   scraper_id BIGINT UNSIGNED NOT NULL,
#   skill_id TINYINT UNSIGNED NOT NULL,
#   skill_value INT UNSIGNED NOT NULL DEFAULT 0, # < 200 000 000
#   FOREIGN KEY (scraper_id) REFERENCES scraper_data(scraper_id) ON DELETE CASCADE,
#   FOREIGN KEY (skill_id) REFERENCES skills(skill_id) ON DELETE CASCADE,
#   PRIMARY KEY (scraper_id, skill_id)
# );
class PlayerSkills(Base):
    __tablename__ = "player_skills"

    scraper_id = Column(
        BIGINT,
        ForeignKey("scraper_data.scraper_id", ondelete="CASCADE"),
        primary_key=True,
    )
    skill_id = Column(
        TINYINT,
        ForeignKey("skills.skill_id", ondelete="CASCADE"),
        primary_key=True,
    )
    skill_value = Column(Integer, nullable=False, default=0)
