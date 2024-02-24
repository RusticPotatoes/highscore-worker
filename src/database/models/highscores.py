from sqlalchemy import (
    BigInteger,
    TinyInteger,
    Column,
    Date,
    DateTime,
    Integer,
    func,
    ForeignKey,
)
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from database.database import Base
from sqlalchemy import String


class PlayerHiscoreData(Base):
    __tablename__ = "playerHiscoreData"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, server_default=func.now())
    ts_date = Column(Date, nullable=True)
    Player_id = Column(Integer, nullable=False)
    total = Column(BigInteger, default=0)
    attack = Column(Integer, default=0)
    defence = Column(Integer, default=0)
    strength = Column(Integer, default=0)
    hitpoints = Column(Integer, default=0)
    ranged = Column(Integer, default=0)
    prayer = Column(Integer, default=0)
    magic = Column(Integer, default=0)
    cooking = Column(Integer, default=0)
    woodcutting = Column(Integer, default=0)
    fletching = Column(Integer, default=0)
    fishing = Column(Integer, default=0)
    firemaking = Column(Integer, default=0)
    crafting = Column(Integer, default=0)
    smithing = Column(Integer, default=0)
    mining = Column(Integer, default=0)
    herblore = Column(Integer, default=0)
    agility = Column(Integer, default=0)
    thieving = Column(Integer, default=0)
    slayer = Column(Integer, default=0)
    farming = Column(Integer, default=0)
    runecraft = Column(Integer, default=0)
    hunter = Column(Integer, default=0)
    construction = Column(Integer, default=0)
    league = Column(Integer, default=0)
    bounty_hunter_hunter = Column(Integer, default=0)
    bounty_hunter_rogue = Column(Integer, default=0)
    cs_all = Column(Integer, default=0)
    cs_beginner = Column(Integer, default=0)
    cs_easy = Column(Integer, default=0)
    cs_medium = Column(Integer, default=0)
    cs_hard = Column(Integer, default=0)
    cs_elite = Column(Integer, default=0)
    cs_master = Column(Integer, default=0)
    lms_rank = Column(Integer, default=0)
    soul_wars_zeal = Column(Integer, default=0)
    abyssal_sire = Column(Integer, default=0)
    alchemical_hydra = Column(Integer, default=0)
    barrows_chests = Column(Integer, default=0)
    bryophyta = Column(Integer, default=0)
    callisto = Column(Integer, default=0)
    cerberus = Column(Integer, default=0)
    chambers_of_xeric = Column(Integer, default=0)
    chambers_of_xeric_challenge_mode = Column(Integer, default=0)
    chaos_elemental = Column(Integer, default=0)
    chaos_fanatic = Column(Integer, default=0)
    commander_zilyana = Column(Integer, default=0)
    corporeal_beast = Column(Integer, default=0)
    crazy_archaeologist = Column(Integer, default=0)
    dagannoth_prime = Column(Integer, default=0)
    dagannoth_rex = Column(Integer, default=0)
    dagannoth_supreme = Column(Integer, default=0)
    deranged_archaeologist = Column(Integer, default=0)
    general_graardor = Column(Integer, default=0)
    giant_mole = Column(Integer, default=0)
    grotesque_guardians = Column(Integer, default=0)
    hespori = Column(Integer, default=0)
    kalphite_queen = Column(Integer, default=0)
    king_black_dragon = Column(Integer, default=0)
    kraken = Column(Integer, default=0)
    kreearra = Column(Integer, default=0)
    kril_tsutsaroth = Column(Integer, default=0)
    mimic = Column(Integer, default=0)
    nex = Column(Integer, default=0)
    nightmare = Column(Integer, default=0)
    phosanis_nightmare = Column(Integer, default=0)
    obor = Column(Integer, default=0)
    phantom_muspah = Column(Integer, default=0)
    sarachnis = Column(Integer, default=0)
    scorpia = Column(Integer, default=0)
    skotizo = Column(Integer, default=0)
    tempoross = Column(Integer, default=0)
    the_gauntlet = Column(Integer, default=0)
    the_corrupted_gauntlet = Column(Integer, default=0)
    theatre_of_blood = Column(Integer, default=0)
    theatre_of_blood_hard = Column(Integer, default=0)
    thermonuclear_smoke_devil = Column(Integer, default=0)
    tombs_of_amascut = Column(Integer, default=0)
    tombs_of_amascut_expert = Column(Integer, default=0)
    tzkal_zuk = Column(Integer, default=0)
    tztok_jad = Column(Integer, default=0)
    venenatis = Column(Integer, default=0)
    vetion = Column(Integer, default=0)
    vorkath = Column(Integer, default=0)
    wintertodt = Column(Integer, default=0)
    zalcano = Column(Integer, default=0)
    zulrah = Column(Integer, default=0)
    rifts_closed = Column(Integer, default=0)
    artio = Column(Integer, default=0)
    calvarion = Column(Integer, default=0)
    duke_sucellus = Column(Integer, default=0)
    spindel = Column(Integer, default=0)
    the_leviathan = Column(Integer, default=0)
    the_whisperer = Column(Integer, default=0)
    vardorvis = Column(Integer, default=0)


# CREATE TABLE scraper_data (
#   scraper_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
#   created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   player_id SMALLINT UNSIGNED NOT NULL,
#   record_date DATE AS (DATE(created_at)) STORED,
#   UNIQUE KEY unique_player_per_day (player_id, record_date)
# );
class ScraperData(Base):
    __tablename__ = "scraper_data"

    scraper_id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    player_id = Column(SmallInteger, nullable=False)
    record_date = Column(Date, nullable=True, server_onupdate=func.current_date())
    player_skills = relationship("PlayerSkills", back_populates="scraper_data")
    __table_args__ = (
        UniqueConstraint("player_id", "record_date", name="unique_player_per_day"),
    )


# CREATE TABLE skills (
#   skill_id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, # < 255
#   skill_name VARCHAR(50) NOT NULL,
#   UNIQUE KEY unique_skill_name (skill_name)
# );
class Skills(Base):
    __tablename__ = "skills"

    skill_id = Column(TinyInteger, primary_key=True, autoincrement=True)
    skill_name = Column(String(50), nullable=False)

    player_skills = relationship("PlayerSkills", back_populates="skill")

    __table_args__ = (UniqueConstraint("skill_name", name="unique_skill_name"),)


# done
# CREATE TABLE activities (
#   activity_id TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, # < 255
#   activity_name VARCHAR(50) NOT NULL,
#   UNIQUE KEY unique_activity_name (activity_name)
# );
class Activities(Base):
    __tablename__ = "activities"

    activity_id = Column(TinyInteger, primary_key=True, autoincrement=True)
    activity_name = Column(String(50), nullable=False)

    __table_args__ = (UniqueConstraint("activity_name", name="unique_activity_name"),)


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
        BigInteger,
        ForeignKey("scraper_data.scraper_id", ondelete="CASCADE"),
        primary_key=True,
    )
    skill_id = Column(
        TinyInteger,
        ForeignKey("skills.skill_id", ondelete="CASCADE"),
        primary_key=True,
    )
    skill_value = Column(Integer, nullable=False, default=0)

    scraper_data = relationship("ScraperData", back_populates="player_skills")
    skill = relationship("Skills", back_populates="player_skills")


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
        BigInteger,
        ForeignKey("scraper_data.scraper_id", ondelete="CASCADE"),
        primary_key=True,
    )
    activity_id = Column(
        TinyInteger,
        ForeignKey("activities.activity_id", ondelete="CASCADE"),
        primary_key=True,
    )
    activity_value = Column(Integer, nullable=False, default=0)
