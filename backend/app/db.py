from contextlib import suppress
from datetime import datetime, timezone
from html import unescape
import base64
import re
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
import json
import httpx

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.config import get_settings
from app.schemas import (
    AgentItem,
    AgentUpsert,
    CommercialCompetitionFeed,
    CommercialFeed,
    CompetitionItem,
    CompetitionUpsert,
    FixtureItem,
    HighlightCard,
    HighlightUpsert,
    ImportPayload,
    ManagedMatch,
    MarketValuationItem,
    MatchEventInput,
    MatchUpsert,
    PlayerProfile,
    PlayerProfileUpsert,
    PlayerStatItem,
    PlayerTransferCard,
    PlayerValuationUpdate,
    RepositoryMeta,
    RepositoryOverview,
    SourceAssetItem,
    SourceAssetUpsert,
    SourceIngestionRequest,
    StandingItem,
    StatItem,
    TeamItem,
    TeamUpsert,
    User,
    UserCreate,
    DynamicDataIngest,
    DynamicTableItem,
    DynamicTablePreview,
    ImageJsonExtractionResponse,
    AIAnalysisRequest,
)
from app.source_data import ASSIST_LEADERS, COMPETITION, HIGHLIGHTS, MATCHES, SOURCE_ASSETS, STANDINGS, TEAMS


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS repository_meta (
    meta_key TEXT PRIMARY KEY,
    brand TEXT,
    tagline TEXT,
    hero_badge TEXT,
    hero_title TEXT,
    hero_body TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS agents (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    agency_name TEXT,
    fifa_license_number TEXT,
    whatsapp_number TEXT,
    email TEXT,
    linkedin_profile TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    hashed_password TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'guest',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS competitions (
    id SERIAL PRIMARY KEY
);
ALTER TABLE competitions ADD COLUMN IF NOT EXISTS name TEXT;
ALTER TABLE competitions ADD COLUMN IF NOT EXISTS season_year INTEGER;
ALTER TABLE competitions ADD COLUMN IF NOT EXISTS country TEXT;
ALTER TABLE competitions ADD COLUMN IF NOT EXISTS current_season TEXT;
ALTER TABLE competitions ADD COLUMN IF NOT EXISTS logo TEXT;
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    short_name TEXT NOT NULL,
    city TEXT NOT NULL,
    icon_text TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    position TEXT,
    UNIQUE (team_id, name)
);
ALTER TABLE players ADD COLUMN IF NOT EXISTS market_value DECIMAL(12, 2) DEFAULT 0;
ALTER TABLE players ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'MWK';
ALTER TABLE players ADD COLUMN IF NOT EXISTS is_premium BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS height_cm INTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS weight_kg INTEGER;
ALTER TABLE players ADD COLUMN IF NOT EXISTS preferred_foot TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS date_of_birth DATE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS citizenship TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS contract_expires DATE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS joining_date DATE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS is_transfer_listed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS loan_status BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE players ADD COLUMN IF NOT EXISTS agent_id INTEGER REFERENCES agents(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS player_valuation_history (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    valuation DECIMAL(12, 2) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS repo_highlights (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL UNIQUE,
    body TEXT NOT NULL,
    icon_key TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    competition_id INTEGER REFERENCES competitions(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS source_assets (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    label TEXT NOT NULL
);
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS source_domain TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS source_kind TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS mime_type TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS tags TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS scrape_status TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS external_id TEXT;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS competition_id INTEGER REFERENCES competitions(id) ON DELETE SET NULL;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS is_manual BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS is_premium BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS price DECIMAL(10, 2) DEFAULT 0;
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE source_assets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE TABLE IF NOT EXISTS matches (
    id SERIAL PRIMARY KEY,
    competition_id INTEGER NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    week_no INTEGER,
    week_label TEXT NOT NULL,
    match_date DATE,
    home_team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    away_team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    home_score INTEGER NOT NULL,
    away_score INTEGER NOT NULL,
    source_image TEXT NOT NULL UNIQUE
);
ALTER TABLE matches ADD COLUMN IF NOT EXISTS source_asset_id INTEGER REFERENCES source_assets(id) ON DELETE SET NULL;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS notes TEXT;
CREATE TABLE IF NOT EXISTS match_events (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    player_name TEXT NOT NULL,
    minute TEXT NOT NULL,
    event_type TEXT NOT NULL,
    is_penalty BOOLEAN NOT NULL DEFAULT FALSE,
    is_own_goal BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (match_id, team_id, player_name, minute, event_type)
);
CREATE TABLE IF NOT EXISTS standings_snapshots (
    id SERIAL PRIMARY KEY,
    competition_id INTEGER NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    week_label TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    rank INTEGER NOT NULL,
    team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    played INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    draws INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    goals_for INTEGER NOT NULL,
    goals_against INTEGER NOT NULL,
    goal_difference INTEGER NOT NULL,
    points INTEGER NOT NULL,
    status TEXT NOT NULL,
    form TEXT, -- New: Store W,D,L,W,W
    UNIQUE (competition_id, week_label, snapshot_date, team_id)
);

ALTER TABLE matches ADD COLUMN IF NOT EXISTS venue TEXT;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS attendance INTEGER;
ALTER TABLE matches ADD COLUMN IF NOT EXISTS half_time_score TEXT;
CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    competition_id INTEGER NOT NULL REFERENCES competitions(id) ON DELETE CASCADE,
    season_year INTEGER NOT NULL,
    stat_type TEXT NOT NULL,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    value INTEGER NOT NULL,
    matches_played INTEGER,
    UNIQUE (competition_id, season_year, stat_type, player_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS competitions_name_season_year_idx ON competitions (name, season_year);
CREATE INDEX IF NOT EXISTS repo_highlights_competition_idx ON repo_highlights (competition_id);
CREATE INDEX IF NOT EXISTS source_assets_category_idx ON source_assets (category);
CREATE INDEX IF NOT EXISTS source_assets_domain_idx ON source_assets (source_domain);
CREATE INDEX IF NOT EXISTS source_assets_competition_idx ON source_assets (competition_id);
CREATE INDEX IF NOT EXISTS matches_competition_idx ON matches (competition_id);
"""

DEFAULT_META = RepositoryMeta(
    brand="NyasaSport Repo",
    tagline="Structured football data repository",
    hero_badge="Manual Curation | URL Imports | Commercial API",
    hero_title="Competition Data Workspace",
    hero_body="Store images, edit records manually, and ingest categorized source URLs for downstream products.",
)


class PostgresService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.connected = False

    def _connect(self):
        if not self.settings.database_url:
            raise ValueError("DATABASE_URL is not configured")
        return psycopg.connect(self.settings.database_url, row_factory=dict_row)

    def initialize(self) -> bool:
        if not self.settings.database_url:
            return False
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
                self._upsert_repository_meta(cur, DEFAULT_META)
                if self.settings.init_db_on_startup:
                    self._seed(cur)
            conn.commit()
        self.connected = True
        return True

    def _seed(self, cur) -> None:
        competition_id = self._save_competition_tx(
            cur,
            CompetitionUpsert(
                name=COMPETITION["name"],
                season_year=COMPETITION["season_year"],
                country=COMPETITION["country"],
                current_season=str(COMPETITION["season_year"]),
                logo=COMPETITION["logo"],
            ),
        )

        for highlight in HIGHLIGHTS:
            self._save_highlight_tx(
                cur,
                HighlightUpsert(
                    title=highlight["title"],
                    body=highlight["body"],
                    icon_key=highlight["icon_key"],
                    sort_order=highlight["sort_order"],
                    competition_id=competition_id,
                ),
            )

        team_ids: dict[str, int] = {}
        for team in TEAMS:
            team_ids[team["name"]] = self._ensure_team(
                cur, team["name"], team["short_name"], team["city"], team["icon_text"]
            )

        for asset in SOURCE_ASSETS:
            self._save_source_asset_tx(
                cur,
                SourceAssetUpsert(
                    filename=asset["filename"],
                    category=asset["category"],
                    label=asset["label"],
                    competition_id=competition_id,
                ),
            )

        for match in MATCHES:
            self._save_match_tx(
                cur,
                MatchUpsert(
                    competition_id=competition_id,
                    week_label=match["week_label"],
                    home_team=match["home_team"],
                    away_team=match["away_team"],
                    home_score=match["home_score"],
                    away_score=match["away_score"],
                    source_image=match["source_image"],
                    events=[MatchEventInput(**event) for event in match["events"]],
                ),
            )

        for item in ASSIST_LEADERS:
            team_id = team_ids.get(item["team"]) or self._ensure_team(cur, item["team"], None, None, None)
            player_id = self._ensure_player(cur, team_id, item["player"])
            cur.execute(
                """
                INSERT INTO player_stats (
                    competition_id, season_year, stat_type, player_id, value, matches_played
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (competition_id, season_year, stat_type, player_id) DO UPDATE
                SET value = EXCLUDED.value,
                    matches_played = EXCLUDED.matches_played
                """,
                (competition_id, 2025, "assists", player_id, item["value"], item["matches_played"]),
            )

    def _split_tags(self, value: str | None) -> list[str]:
        if not value:
            return []
        return [item for item in (part.strip() for part in value.split(",")) if item]

    def _join_tags(self, tags: list[str]) -> str | None:
        cleaned = []
        seen: set[str] = set()
        for tag in tags:
            normalized = tag.strip().lower()
            if normalized and normalized not in seen:
                seen.add(normalized)
                cleaned.append(normalized)
        return ", ".join(cleaned) or None

    def _map_source_asset(self, row: dict[str, Any]) -> SourceAssetItem:
        payload = dict(row)
        payload["tags"] = self._split_tags(payload.get("tags"))
        return SourceAssetItem(**payload)

    def _competition_name(self, cur, competition_id: int) -> str:
        cur.execute("SELECT name FROM competitions WHERE id = %s", (competition_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Competition {competition_id} not found")
        return row["name"]

    def _ensure_team(self, cur, name: str, short_name: str | None, city: str | None, icon_text: str | None) -> int:
        short_value = short_name or "".join(part[0] for part in name.split()[:3]).upper() or name[:3].upper()
        city_value = city or "Unknown"
        icon_value = icon_text or short_value[:2]
        cur.execute(
            """
            INSERT INTO teams (name, short_name, city, icon_text)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE
            SET short_name = EXCLUDED.short_name,
                city = EXCLUDED.city,
                icon_text = EXCLUDED.icon_text
            RETURNING id
            """,
            (name, short_value, city_value, icon_value),
        )
        return cur.fetchone()["id"]

    def _ensure_player(
        self,
        cur,
        team_id: int,
        player_name: str,
        player_lookup: dict[tuple[int, str], int] | None = None,
    ) -> int:
        cache_key = (team_id, player_name)
        if player_lookup is not None and cache_key in player_lookup:
            return player_lookup[cache_key]
        cur.execute(
            """
            INSERT INTO players (team_id, name)
            VALUES (%s, %s)
            ON CONFLICT (team_id, name) DO UPDATE
            SET name = EXCLUDED.name
            RETURNING id
            """,
            (team_id, player_name),
        )
        player_id = cur.fetchone()["id"]
        if player_lookup is not None:
            player_lookup[cache_key] = player_id
        return player_id

    def _upsert_repository_meta(self, cur, meta: RepositoryMeta) -> dict[str, Any]:
        cur.execute(
            """
            INSERT INTO repository_meta (meta_key, brand, tagline, hero_badge, hero_title, hero_body, updated_at)
            VALUES ('default', %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (meta_key) DO UPDATE
            SET brand = COALESCE(EXCLUDED.brand, repository_meta.brand),
                tagline = COALESCE(EXCLUDED.tagline, repository_meta.tagline),
                hero_badge = COALESCE(EXCLUDED.hero_badge, repository_meta.hero_badge),
                hero_title = COALESCE(EXCLUDED.hero_title, repository_meta.hero_title),
                hero_body = COALESCE(EXCLUDED.hero_body, repository_meta.hero_body),
                updated_at = NOW()
            RETURNING brand, tagline, hero_badge, hero_title, hero_body
            """,
            (meta.brand, meta.tagline, meta.hero_badge, meta.hero_title, meta.hero_body),
        )
        return cur.fetchone()

    def get_repository_meta(self) -> RepositoryMeta:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT brand, tagline, hero_badge, hero_title, hero_body
                FROM repository_meta
                WHERE meta_key = 'default'
                """
            )
            row = cur.fetchone()
        if not row:
            return DEFAULT_META
        return RepositoryMeta(**row)

    def save_repository_meta(self, payload: RepositoryMeta) -> RepositoryMeta:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._upsert_repository_meta(cur, payload)
            conn.commit()
        return RepositoryMeta(**row)

    def _save_competition_tx(
        self,
        cur,
        payload: CompetitionUpsert,
        competition_id: int | None = None,
    ) -> dict[str, Any]:
        if competition_id is None:
            cur.execute(
                """
                INSERT INTO competitions (name, season_year, country, current_season, logo)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name, season_year) DO UPDATE
                SET country = EXCLUDED.country,
                    current_season = EXCLUDED.current_season,
                    logo = EXCLUDED.logo
                RETURNING id, name, season_year, country, current_season, logo
                """,
                (
                    payload.name,
                    payload.season_year,
                    payload.country,
                    payload.current_season,
                    payload.logo,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE competitions
                SET name = %s,
                    season_year = %s,
                    country = %s,
                    current_season = %s,
                    logo = %s
                WHERE id = %s
                RETURNING id, name, season_year, country, current_season, logo
                """,
                (
                    payload.name,
                    payload.season_year,
                    payload.country,
                    payload.current_season,
                    payload.logo,
                    competition_id,
                ),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("Competition could not be saved")
        return row

    def list_competitions(self) -> list[CompetitionItem]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, season_year, country, current_season, logo FROM competitions ORDER BY season_year DESC, name"
            )
            return [CompetitionItem(**row) for row in cur.fetchall()]

    def save_competition(self, payload: CompetitionUpsert, competition_id: int | None = None) -> CompetitionItem:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._save_competition_tx(cur, payload, competition_id)
            conn.commit()
        return CompetitionItem(**row)

    def _save_team_tx(self, cur, payload: TeamUpsert, team_id: int | None = None) -> dict[str, Any]:
        if team_id is None:
            resolved_id = self._ensure_team(cur, payload.name, payload.short_name, payload.city, payload.icon_text)
        else:
            cur.execute(
                """
                UPDATE teams
                SET name = %s,
                    short_name = %s,
                    city = %s,
                    icon_text = %s
                WHERE id = %s
                RETURNING id
                """,
                (
                    payload.name,
                    payload.short_name or "".join(part[0] for part in payload.name.split()[:3]).upper() or payload.name[:3].upper(),
                    payload.city or "Unknown",
                    payload.icon_text or (payload.short_name or payload.name[:2]).upper()[:2],
                    team_id,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Team could not be saved")
            resolved_id = row["id"]
        cur.execute("SELECT id, name, city, icon_text FROM teams WHERE id = %s", (resolved_id,))
        return cur.fetchone()

    def list_teams(self) -> list[TeamItem]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, name, city, icon_text FROM teams ORDER BY name")
            return [TeamItem(**row) for row in cur.fetchall()]

    def save_team(self, payload: TeamUpsert, team_id: int | None = None) -> TeamItem:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._save_team_tx(cur, payload, team_id)
            conn.commit()
        return TeamItem(**row)

    def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username,))
            return cur.fetchone()

    def create_user(self, username: str, email: str, hashed_password: str, role: str) -> User:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (username, email, hashed_password, role)
                VALUES (%s, %s, %s, %s)
                RETURNING id, username, email, role, is_active
                """,
                (username, email, hashed_password, role),
            )
            row = cur.fetchone()
            conn.commit()
            return User(**row)

    def list_agents(self) -> list[AgentItem]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, name, agency_name, fifa_license_number, whatsapp_number, email, linkedin_profile FROM agents ORDER BY name")
            return [AgentItem(**row) for row in cur.fetchall()]

    def save_agent(self, payload: AgentUpsert, agent_id: int | None = None) -> AgentItem:
        with self._connect() as conn, conn.cursor() as cur:
            if agent_id is None:
                cur.execute(
                    """
                    INSERT INTO agents (name, agency_name, fifa_license_number, whatsapp_number, email, linkedin_profile)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id, name, agency_name, fifa_license_number, whatsapp_number, email, linkedin_profile
                    """,
                    (payload.name, payload.agency_name, payload.fifa_license_number, payload.whatsapp_number, payload.email, payload.linkedin_profile),
                )
            else:
                cur.execute(
                    """
                    UPDATE agents
                    SET name = %s, agency_name = %s, fifa_license_number = %s, whatsapp_number = %s, email = %s, linkedin_profile = %s
                    WHERE id = %s
                    RETURNING id, name, agency_name, fifa_license_number, whatsapp_number, email, linkedin_profile
                    """,
                    (payload.name, payload.agency_name, payload.fifa_license_number, payload.whatsapp_number, payload.email, payload.linkedin_profile, agent_id),
                )
            row = cur.fetchone()
            conn.commit()
            return AgentItem(**row)

    def get_player_profile(self, player_id: int) -> PlayerProfile | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.name, p.team_id, t.name AS team_name, p.position, p.height_cm, p.weight_kg,
                       p.preferred_foot, p.date_of_birth::text, p.citizenship, p.contract_expires::text,
                       p.joining_date::text, p.is_transfer_listed, p.loan_status, p.market_value, p.currency,
                       p.agent_id, a.name AS agent_name
                FROM players p
                JOIN teams t ON t.id = p.team_id
                LEFT JOIN agents a ON a.id = p.agent_id
                WHERE p.id = %s
                """,
                (player_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return PlayerProfile(**row)

    def save_player_profile(self, payload: PlayerProfileUpsert, player_id: int | None = None) -> PlayerProfile:
        with self._connect() as conn, conn.cursor() as cur:
            if player_id is None:
                cur.execute(
                    """
                    INSERT INTO players (name, team_id, position, height_cm, weight_kg, preferred_foot, 
                                        date_of_birth, citizenship, contract_expires, joining_date, 
                                        is_transfer_listed, loan_status, agent_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_id, name) DO UPDATE SET
                        position = EXCLUDED.position,
                        height_cm = EXCLUDED.height_cm,
                        weight_kg = EXCLUDED.weight_kg,
                        preferred_foot = EXCLUDED.preferred_foot,
                        date_of_birth = EXCLUDED.date_of_birth,
                        citizenship = EXCLUDED.citizenship,
                        contract_expires = EXCLUDED.contract_expires,
                        joining_date = EXCLUDED.joining_date,
                        is_transfer_listed = EXCLUDED.is_transfer_listed,
                        loan_status = EXCLUDED.loan_status,
                        agent_id = EXCLUDED.agent_id
                    RETURNING id
                    """,
                    (payload.name, payload.team_id, payload.position, payload.height_cm, payload.weight_kg, 
                     payload.preferred_foot, payload.date_of_birth, payload.citizenship, 
                     payload.contract_expires, payload.joining_date, payload.is_transfer_listed, 
                     payload.loan_status, payload.agent_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE players
                    SET name = %s, team_id = %s, position = %s, height_cm = %s, weight_kg = %s, 
                        preferred_foot = %s, date_of_birth = %s, citizenship = %s, 
                        contract_expires = %s, joining_date = %s, is_transfer_listed = %s, 
                        loan_status = %s, agent_id = %s
                    WHERE id = %s
                    RETURNING id
                    """,
                    (payload.name, payload.team_id, payload.position, payload.height_cm, payload.weight_kg, 
                     payload.preferred_foot, payload.date_of_birth, payload.citizenship, 
                     payload.contract_expires, payload.joining_date, payload.is_transfer_listed, 
                     payload.loan_status, payload.agent_id, player_id),
                )
            res = cur.fetchone()
            conn.commit()
            return self.get_player_profile(res["id"])

    def get_player_transfer_card(self, player_id: int) -> PlayerTransferCard | None:
        profile = self.get_player_profile(player_id)
        if not profile:
            return None
        
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT ps.stat_type AS label, t.name AS team, ps.value, ps.matches_played
                FROM player_stats ps
                JOIN teams t ON t.id = (SELECT team_id FROM players WHERE id = %s)
                WHERE ps.player_id = %s
                """,
                (player_id, player_id),
            )
            stats_rows = cur.fetchall()
            # Reformat to PlayerStatItem
            stats = []
            for r in stats_rows:
                stats.append(PlayerStatItem(player=profile.name, team=r["team"], value=r["value"], matches_played=r["matches_played"]))

            cur.execute(
                """
                SELECT valuation::float AS valuation, recorded_at::text AS date
                FROM player_valuation_history
                WHERE player_id = %s
                ORDER BY recorded_at ASC
                """,
                (player_id,),
            )
            valuation_history = cur.fetchall()

            # For highlights, we could fetch match events like goals/assists
            cur.execute(
                """
                SELECT me.event_type, m.week_label, ht.name AS home, at.name AS away, m.match_date::text
                FROM match_events me
                JOIN matches m ON m.id = me.match_id
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE me.player_name = %s
                ORDER BY m.match_date DESC
                LIMIT 5
                """,
                (profile.name,),
            )
            highlights = [f"{r['event_type'].capitalize()} vs {r['away'] if r['home'] == profile.team_name else r['home']} ({r['week_label']})" for r in cur.fetchall()]

        return PlayerTransferCard(
            player=profile,
            stats=stats,
            valuation_history=valuation_history,
            highlights=highlights
        )

    def _save_highlight_tx(self, cur, payload: HighlightUpsert, highlight_id: int | None = None) -> dict[str, Any]:
        if highlight_id is None:
            cur.execute(
                """
                INSERT INTO repo_highlights (title, body, icon_key, sort_order, competition_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (title) DO UPDATE
                SET body = EXCLUDED.body,
                    icon_key = EXCLUDED.icon_key,
                    sort_order = EXCLUDED.sort_order,
                    competition_id = EXCLUDED.competition_id
                RETURNING id, title, body, icon_key, sort_order, competition_id
                """,
                (payload.title, payload.body, payload.icon_key, payload.sort_order, payload.competition_id),
            )
        else:
            cur.execute(
                """
                UPDATE repo_highlights
                SET title = %s,
                    body = %s,
                    icon_key = %s,
                    sort_order = %s,
                    competition_id = %s
                WHERE id = %s
                RETURNING id, title, body, icon_key, sort_order, competition_id
                """,
                (payload.title, payload.body, payload.icon_key, payload.sort_order, payload.competition_id, highlight_id),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("Highlight could not be saved")
        return row

    def list_highlights(self, competition_id: int | None = None) -> list[dict[str, Any]]:
        with self._connect() as conn, conn.cursor() as cur:
            if competition_id is None:
                cur.execute(
                    """
                    SELECT rh.id, rh.title, rh.body, rh.icon_key, rh.sort_order, rh.competition_id,
                           c.name AS competition_name
                    FROM repo_highlights rh
                    LEFT JOIN competitions c ON c.id = rh.competition_id
                    ORDER BY rh.sort_order, rh.id
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT rh.id, rh.title, rh.body, rh.icon_key, rh.sort_order, rh.competition_id,
                           c.name AS competition_name
                    FROM repo_highlights rh
                    LEFT JOIN competitions c ON c.id = rh.competition_id
                    WHERE rh.competition_id IS NULL OR rh.competition_id = %s
                    ORDER BY rh.sort_order, rh.id
                    """,
                    (competition_id,),
                )
            return [dict(row) for row in cur.fetchall()]

    def save_highlight(self, payload: HighlightUpsert, highlight_id: int | None = None) -> dict[str, Any]:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._save_highlight_tx(cur, payload, highlight_id)
            conn.commit()
        return row

    def _save_source_asset_tx(
        self,
        cur,
        payload: SourceAssetUpsert,
        asset_id: int | None = None,
    ) -> dict[str, Any]:
        tag_value = self._join_tags(payload.tags)
        source_domain = urlparse(payload.source_url).netloc.lower() if payload.source_url else None
        if asset_id is None:
            cur.execute(
                """
                INSERT INTO source_assets (
                    filename, category, label, source_url, source_domain, source_kind, image_url, thumbnail_url,
                    mime_type, notes, tags, scrape_status, external_id, competition_id, is_manual, 
                    is_premium, price, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (filename) DO UPDATE
                SET category = EXCLUDED.category,
                    label = EXCLUDED.label,
                    source_url = EXCLUDED.source_url,
                    source_domain = EXCLUDED.source_domain,
                    source_kind = EXCLUDED.source_kind,
                    image_url = EXCLUDED.image_url,
                    thumbnail_url = EXCLUDED.thumbnail_url,
                    mime_type = EXCLUDED.mime_type,
                    notes = EXCLUDED.notes,
                    tags = EXCLUDED.tags,
                    scrape_status = EXCLUDED.scrape_status,
                    external_id = EXCLUDED.external_id,
                    competition_id = EXCLUDED.competition_id,
                    is_manual = EXCLUDED.is_manual,
                    is_premium = EXCLUDED.is_premium,
                    price = EXCLUDED.price,
                    updated_at = NOW()
                RETURNING id, filename, category, label, source_url, source_domain, source_kind,
                          image_url, thumbnail_url, mime_type, notes, tags, scrape_status, external_id,
                          competition_id, is_manual, is_premium, price, created_at::text, updated_at::text
                """,
                (
                    payload.filename,
                    payload.category,
                    payload.label,
                    payload.source_url,
                    source_domain,
                    payload.source_kind,
                    payload.image_url,
                    payload.thumbnail_url,
                    payload.mime_type,
                    payload.notes,
                    tag_value,
                    payload.scrape_status,
                    payload.external_id,
                    payload.competition_id,
                    payload.is_manual,
                    payload.is_premium,
                    payload.price,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE source_assets
                SET filename = %s,
                    category = %s,
                    label = %s,
                    source_url = %s,
                    source_domain = %s,
                    source_kind = %s,
                    image_url = %s,
                    thumbnail_url = %s,
                    mime_type = %s,
                    notes = %s,
                    tags = %s,
                    scrape_status = %s,
                    external_id = %s,
                    competition_id = %s,
                    is_manual = %s,
                    is_premium = %s,
                    price = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, filename, category, label, source_url, source_domain, source_kind,
                          image_url, thumbnail_url, mime_type, notes, tags, scrape_status, external_id,
                          competition_id, is_manual, is_premium, price, created_at::text, updated_at::text
                """,
                (
                    payload.filename,
                    payload.category,
                    payload.label,
                    payload.source_url,
                    source_domain,
                    payload.source_kind,
                    payload.image_url,
                    payload.thumbnail_url,
                    payload.mime_type,
                    payload.notes,
                    tag_value,
                    payload.scrape_status,
                    payload.external_id,
                    payload.competition_id,
                    payload.is_manual,
                    payload.is_premium,
                    payload.price,
                    asset_id,
                ),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("Source asset could not be saved")
        return row

    def list_source_assets(
        self,
        competition_id: int | None = None,
        category: str | None = None,
        domain: str | None = None,
    ) -> list[SourceAssetItem]:
        filters = []
        params: list[Any] = []
        if competition_id is not None:
            filters.append("(competition_id IS NULL OR competition_id = %s)")
            params.append(competition_id)
        if category:
            filters.append("category = %s")
            params.append(category)
        if domain:
            filters.append("source_domain = %s")
            params.append(domain.lower())
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, filename, category, label, source_url, source_domain, source_kind,
                       image_url, thumbnail_url, mime_type, notes, tags, scrape_status, external_id,
                       competition_id, is_manual, is_premium, price, created_at::text, updated_at::text
                FROM source_assets
                {where_clause}
                ORDER BY updated_at DESC, id DESC
                """,
                params,
            )
            return [self._map_source_asset(row) for row in cur.fetchall()]

    def save_source_asset(self, payload: SourceAssetUpsert, asset_id: int | None = None) -> SourceAssetItem:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._save_source_asset_tx(cur, payload, asset_id)
            conn.commit()
        return self._map_source_asset(row)

    def _save_match_tx(self, cur, payload: MatchUpsert, match_id: int | None = None) -> dict[str, Any]:
        self._competition_name(cur, payload.competition_id)
        home_team_id = self._ensure_team(cur, payload.home_team, None, None, None)
        away_team_id = self._ensure_team(cur, payload.away_team, None, None, None)
        source_image = (
            payload.source_image
            or f"{payload.competition_id}-{payload.home_team}-{payload.away_team}-{payload.match_date or 'undated'}"
        )

        if match_id is None:
            cur.execute(
                """
                INSERT INTO matches (
                    competition_id, week_no, week_label, match_date, home_team_id, away_team_id,
                    home_score, away_score, source_image, source_asset_id, image_url, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_image) DO UPDATE
                SET competition_id = EXCLUDED.competition_id,
                    week_no = EXCLUDED.week_no,
                    week_label = EXCLUDED.week_label,
                    match_date = EXCLUDED.match_date,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    source_asset_id = EXCLUDED.source_asset_id,
                    image_url = EXCLUDED.image_url,
                    notes = EXCLUDED.notes
                RETURNING id
                """,
                (
                    payload.competition_id,
                    payload.week_no,
                    payload.week_label,
                    payload.match_date,
                    home_team_id,
                    away_team_id,
                    payload.home_score,
                    payload.away_score,
                    source_image,
                    payload.source_asset_id,
                    payload.image_url,
                    payload.notes,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE matches
                SET competition_id = %s,
                    week_no = %s,
                    week_label = %s,
                    match_date = %s,
                    home_team_id = %s,
                    away_team_id = %s,
                    home_score = %s,
                    away_score = %s,
                    source_image = %s,
                    source_asset_id = %s,
                    image_url = %s,
                    notes = %s
                WHERE id = %s
                RETURNING id
                """,
                (
                    payload.competition_id,
                    payload.week_no,
                    payload.week_label,
                    payload.match_date,
                    home_team_id,
                    away_team_id,
                    payload.home_score,
                    payload.away_score,
                    source_image,
                    payload.source_asset_id,
                    payload.image_url,
                    payload.notes,
                    match_id,
                ),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("Match could not be saved")
        resolved_match_id = row["id"]

        cur.execute("DELETE FROM match_events WHERE match_id = %s", (resolved_match_id,))
        player_lookup: dict[tuple[int, str], int] = {}
        for event in payload.events:
            event_team_id = self._ensure_team(cur, event.team, None, None, None)
            self._ensure_player(cur, event_team_id, event.player, player_lookup)
            cur.execute(
                """
                INSERT INTO match_events (
                    match_id, team_id, player_name, minute, event_type, is_penalty, is_own_goal
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    resolved_match_id,
                    event_team_id,
                    event.player,
                    event.minute,
                    event.event_type,
                    event.is_penalty,
                    event.is_own_goal,
                ),
            )

        cur.execute(
            """
            SELECT m.id, m.competition_id, c.name AS competition_name, m.week_no, m.week_label,
                   m.match_date::text AS match_date, ht.name AS home_team, at.name AS away_team,
                   m.home_score, m.away_score, m.source_image, m.source_asset_id, m.image_url, m.notes
            FROM matches m
            JOIN competitions c ON c.id = m.competition_id
            JOIN teams ht ON ht.id = m.home_team_id
            JOIN teams at ON at.id = m.away_team_id
            WHERE m.id = %s
            """,
            (resolved_match_id,),
        )
        match_row = dict(cur.fetchone())
        cur.execute(
            """
            SELECT t.name AS team, me.player_name AS player, me.minute, me.event_type, me.is_penalty, me.is_own_goal
            FROM match_events me
            JOIN teams t ON t.id = me.team_id
            WHERE me.match_id = %s
            ORDER BY me.id
            """,
            (resolved_match_id,),
        )
        match_row["events"] = [MatchEventInput(**event) for event in cur.fetchall()]
        return match_row

    def list_matches(self, competition_id: int | None = None) -> list[ManagedMatch]:
        filters = []
        params: list[Any] = []
        if competition_id is not None:
            filters.append("m.competition_id = %s")
            params.append(competition_id)
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT m.id, m.competition_id, c.name AS competition_name, m.week_no, m.week_label,
                       m.match_date::text AS match_date, ht.name AS home_team, at.name AS away_team,
                       m.home_score, m.away_score, m.source_image, m.source_asset_id, m.image_url, m.notes
                FROM matches m
                JOIN competitions c ON c.id = m.competition_id
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                {where_clause}
                ORDER BY m.match_date DESC NULLS LAST, m.id DESC
                """,
                params,
            )
            matches = [dict(row) for row in cur.fetchall()]
            for item in matches:
                cur.execute(
                    """
                    SELECT t.name AS team, me.player_name AS player, me.minute, me.event_type, me.is_penalty, me.is_own_goal
                    FROM match_events me
                    JOIN teams t ON t.id = me.team_id
                    WHERE me.match_id = %s
                    ORDER BY me.id
                    """,
                    (item["id"],),
                )
                item["events"] = [MatchEventInput(**event) for event in cur.fetchall()]
            return [ManagedMatch(**item) for item in matches]

    def save_match(self, payload: MatchUpsert, match_id: int | None = None) -> ManagedMatch:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._save_match_tx(cur, payload, match_id)
            conn.commit()
        return ManagedMatch(**row)

    def update_player_valuation(self, payload: PlayerValuationUpdate) -> MarketValuationItem:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE players
                SET market_value = %s,
                    currency = %s,
                    is_premium = %s
                WHERE id = %s
                """,
                (payload.market_value, payload.currency, payload.is_premium, payload.player_id),
            )
            cur.execute(
                """
                INSERT INTO player_valuation_history (player_id, valuation)
                VALUES (%s, %s)
                """,
                (payload.player_id, payload.market_value),
            )
            conn.commit()
            
            cur.execute(
                """
                SELECT p.id AS player_id, p.name AS player_name, t.name AS team_name, p.position,
                       p.market_value, p.currency, p.is_premium, NOW()::text AS valuation_date
                FROM players p
                JOIN teams t ON t.id = p.team_id
                WHERE p.id = %s
                """,
                (payload.player_id,),
            )
            return MarketValuationItem(**cur.fetchone())

    def list_market_valuations(self) -> list[MarketValuationItem]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id AS player_id, p.name AS player_name, t.name AS team_name, p.position,
                       p.market_value, p.currency, p.is_premium,
                       (SELECT recorded_at::text FROM player_valuation_history WHERE player_id = p.id ORDER BY recorded_at DESC LIMIT 1) AS valuation_date
                FROM players p
                JOIN teams t ON t.id = p.team_id
                ORDER BY p.market_value DESC
                """
            )
            return [MarketValuationItem(**row) for row in cur.fetchall()]

    def ingest_source_url(self, payload: SourceIngestionRequest) -> dict[str, Any]:
        warnings: list[str] = []
        fetched = False
        mime_type = None
        title = payload.label
        image_url = None
        category = payload.category
        notes = payload.notes

        if payload.fetch_preview:
            try:
                request = Request(payload.url, headers={"User-Agent": "Mozilla/5.0 Codex ingestion bot"})
                with urlopen(request, timeout=8) as response:
                    body = response.read(200_000).decode("utf-8", errors="ignore")
                    mime_type = response.headers.get_content_type()
                    title = title or self._extract_title(body)
                    image_url = self._extract_meta_content(body, "og:image")
                    if image_url:
                        image_url = urljoin(payload.url, image_url)
                    description = self._extract_meta_content(body, "description")
                    if description and not notes:
                        notes = description
                    fetched = True
            except Exception as exc:
                warnings.append(f"Preview fetch failed: {exc}")

        categorization = self._categorize_url(payload.url, title)
        filename = self._filename_from_url(payload.url, title, category or categorization["category"])
        saved_asset = self.save_source_asset(
            SourceAssetUpsert(
                filename=filename,
                category=category or categorization["category"],
                label=title or filename,
                source_url=payload.url,
                source_kind=categorization["source_kind"],
                image_url=image_url,
                thumbnail_url=image_url,
                mime_type=mime_type,
                notes=notes,
                is_premium=False,
                price=0.0
            )
        )
        return {
            "asset": saved_asset,
            "categorization": categorization,
            "fetched": fetched,
            "warnings": warnings,
        }

    def _extract_meta_content(self, html: str, property_name: str) -> str | None:
        pattern = re.compile(
            rf'<meta[^>]+(?:property|name)=["\']{re.escape(property_name)}["\'][^>]+content=["\']([^"\']+)["\']',
            re.IGNORECASE,
        )
        match = pattern.search(html)
        return unescape(match.group(1)).strip() if match else None

    def _extract_title(self, html: str) -> str | None:
        match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        return re.sub(r"\s+", " ", unescape(match.group(1))).strip() if match else None

    def _categorize_url(self, url: str, title: str | None = None) -> dict[str, str]:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        path = f"{parsed.path} {parsed.query}".lower()
        text = f"{domain} {path} {(title or '').lower()}"

        category = "article"
        if any(token in text for token in ("standings", "table", "log")):
            category = "standings"
        elif any(token in text for token in ("fixture", "schedule")):
            category = "fixtures"
        elif any(token in text for token in ("result", "match", "live")):
            category = "results"
        elif any(token in text for token in ("assist", "scorer", "stats", "statistics")):
            category = "stats"
        elif any(token in text for token in ("photo", "image", "gallery", ".jpg", ".jpeg", ".png", ".webp")):
            category = "image"
        elif any(token in text for token in ("news", "report")):
            category = "news"

        source_kind = "webpage"
        if any(token in path for token in (".jpg", ".jpeg", ".png", ".webp", ".gif")):
            source_kind = "image"
        elif "flashscore" in domain:
            source_kind = "flashscore"
        elif any(token in domain for token in ("facebook.com", "instagram.com", "x.com", "twitter.com", "youtube.com")):
            source_kind = "social"

        return {
            "domain": domain,
            "category": category,
            "source_kind": source_kind,
        }

    def _filename_from_url(self, url: str, label: str | None, category: str) -> str:
        parsed = urlparse(url)
        candidate = (parsed.path.rsplit("/", 1)[-1] or label or f"{parsed.netloc}-{category}").strip()
        candidate = re.sub(r"[^a-zA-Z0-9._-]+", "-", candidate).strip("-")
        if "." not in candidate:
            candidate = f"{candidate or 'source'}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.html"
        return candidate[:180]

    def get_overview(self, competition_id: int | None = None) -> RepositoryOverview:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, season_year, country, current_season, logo FROM competitions ORDER BY season_year DESC, name"
            )
            competitions = [CompetitionItem(**row) for row in cur.fetchall()]
            if not competitions:
                raise ValueError("Competition data is not initialized")

            active = next((c for c in competitions if c.id == competition_id), competitions[0]) if competition_id else competitions[0]
            meta = self.get_repository_meta()

            cur.execute(
                """
                SELECT title, body, icon_key
                FROM repo_highlights
                WHERE competition_id IS NULL OR competition_id = %s
                ORDER BY sort_order
                """,
                (active.id,),
            )
            highlights = [HighlightCard(**row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT m.id, m.week_label, m.match_date::text AS match_date,
                       ht.name AS home, at.name AS away,
                       m.home_score || '-' || m.away_score AS score
                FROM matches m
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE m.competition_id = %s
                ORDER BY m.match_date DESC NULLS LAST, m.id DESC
                LIMIT 8
                """,
                (active.id,),
            )
            fixtures = [FixtureItem(**row) for row in cur.fetchall()]

            cur.execute("SELECT id, name, city, icon_text FROM teams ORDER BY name LIMIT 8")
            teams = [TeamItem(**row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT ss.rank, t.name AS team, ss.played, ss.wins, ss.draws, ss.losses,
                       ss.goals_for AS gf, ss.goals_against AS ga, ss.goal_difference AS gd,
                       ss.points, ss.status
                FROM standings_snapshots ss
                JOIN teams t ON t.id = ss.team_id
                WHERE ss.competition_id = %s
                  AND (ss.snapshot_date, ss.week_label) IN (
                      SELECT snapshot_date, week_label
                      FROM standings_snapshots
                      WHERE competition_id = %s
                      ORDER BY snapshot_date DESC, week_label DESC
                      LIMIT 1
                  )
                ORDER BY ss.rank
                """,
                (active.id, active.id),
            )
            standings = [StandingItem(**row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT me.player_name AS player, t.name AS team, COUNT(*)::int AS value
                FROM match_events me
                JOIN teams t ON t.id = me.team_id
                JOIN matches m ON m.id = me.match_id
                WHERE me.event_type = 'goal' AND me.is_own_goal = FALSE
                  AND m.competition_id = %s
                GROUP BY me.player_name, t.name
                ORDER BY value DESC, me.player_name
                LIMIT 5
                """,
                (active.id,),
            )
            top_scorers = [PlayerStatItem(**row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT p.name AS player, t.name AS team, ps.value, ps.matches_played
                FROM player_stats ps
                JOIN players p ON p.id = ps.player_id
                JOIN teams t ON t.id = p.team_id
                WHERE ps.competition_id = %s AND ps.stat_type = 'assists'
                ORDER BY ps.value DESC, p.name
                LIMIT 5
                """,
                (active.id,),
            )
            assist_leaders = [PlayerStatItem(**row) for row in cur.fetchall()]

            cur.execute("SELECT COUNT(*)::int AS count FROM teams")
            team_count = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(*)::int AS count FROM players")
            player_count = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(*)::int AS count FROM matches WHERE competition_id = %s", (active.id,))
            match_count = cur.fetchone()["count"]
            cur.execute("SELECT COUNT(*)::int AS count FROM source_assets")
            asset_count = cur.fetchone()["count"]
            cur.execute(
                """
                SELECT id, filename, category, label, source_url, source_domain, source_kind,
                       image_url, thumbnail_url, mime_type, notes, tags, scrape_status, external_id,
                       competition_id, is_manual, is_premium, price, created_at::text, updated_at::text
                FROM source_assets
                WHERE competition_id IS NULL OR competition_id = %s
                ORDER BY updated_at DESC, id DESC
                LIMIT 8
                """,
                (active.id,),
            )
            source_assets = [self._map_source_asset(row) for row in cur.fetchall()]
            
            cur.execute(
                """
                SELECT p.id AS player_id, p.name AS player_name, t.name AS team_name, p.position,
                       p.market_value, p.currency, p.is_premium,
                       (SELECT recorded_at::text FROM player_valuation_history WHERE player_id = p.id ORDER BY recorded_at DESC LIMIT 1) AS valuation_date
                FROM players p
                JOIN teams t ON t.id = p.team_id
                WHERE p.market_value > 0
                ORDER BY p.market_value DESC
                LIMIT 10
                """
            )
            market_valuations = [MarketValuationItem(**row) for row in cur.fetchall()]

        stats = [
            StatItem(label="Tracked Teams", value=f"{team_count:02d}"),
            StatItem(label="Named Players", value=f"{player_count:02d}"),
            StatItem(label="Loaded Matches", value=f"{match_count:02d}"),
            StatItem(label="Source Assets", value=f"{asset_count:02d}"),
        ]

        return RepositoryOverview(
            brand=meta.brand or DEFAULT_META.brand or "",
            tagline=meta.tagline or f"{active.name} data repository",
            hero_badge=meta.hero_badge or DEFAULT_META.hero_badge or "",
            hero_title=meta.hero_title or f"{active.name} Repository",
            hero_body=meta.hero_body or DEFAULT_META.hero_body or "",
            active_competition=active,
            stats=stats,
            highlights=highlights,
            fixtures=fixtures,
            teams=teams,
            competitions=competitions,
            standings=standings,
            top_scorers=top_scorers,
            assist_leaders=assist_leaders,
            source_assets=source_assets,
            market_valuations=market_valuations
        )

    def get_commercial_feed(self) -> CommercialFeed:
        repository = self.get_repository_meta()
        competitions = self.list_competitions()
        competition_feeds: list[CommercialCompetitionFeed] = []
        all_matches = self.list_matches()
        all_assets = self.list_source_assets()

        for competition in competitions:
            competition_matches = [match for match in all_matches if match.competition_id == competition.id][:12]
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ss.rank, t.name AS team, ss.played, ss.wins, ss.draws, ss.losses,
                           ss.goals_for AS gf, ss.goals_against AS ga, ss.goal_difference AS gd,
                           ss.points, ss.status
                    FROM standings_snapshots ss
                    JOIN teams t ON t.id = ss.team_id
                    WHERE ss.competition_id = %s
                      AND (ss.snapshot_date, ss.week_label) IN (
                          SELECT snapshot_date, week_label
                          FROM standings_snapshots
                          WHERE competition_id = %s
                          ORDER BY snapshot_date DESC, week_label DESC
                          LIMIT 1
                      )
                    ORDER BY ss.rank
                    """,
                    (competition.id, competition.id),
                )
                latest_standings = [StandingItem(**row) for row in cur.fetchall()]
            competition_assets = [
                asset for asset in all_assets if asset.competition_id in (None, competition.id)
            ][:12]
            competition_feeds.append(
                CommercialCompetitionFeed(
                    competition=competition,
                    latest_standings=latest_standings,
                    recent_matches=competition_matches,
                    source_assets=competition_assets,
                )
            )

        return CommercialFeed(repository=repository, competitions=competition_feeds)

    def _normalize_identifier(self, value: str) -> str:
        normalized = re.sub(r"[^a-z0-9_]+", "_", value.strip().lower()).strip("_")
        if not normalized:
            return ""
        if normalized[0].isdigit():
            normalized = f"n_{normalized}"
        return normalized

    def _dynamic_table_names(self, cur) -> set[str]:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        core_tables = {
            "repository_meta",
            "agents",
            "users",
            "competitions",
            "teams",
            "players",
            "player_valuation_history",
            "repo_highlights",
            "source_assets",
            "matches",
            "match_events",
            "standings_snapshots",
            "player_stats",
        }
        return {
            row["table_name"]
            for row in cur.fetchall()
            if row["table_name"] not in core_tables and not row["table_name"].startswith("pg_")
        }

    def _infer_dynamic_column_type(self, values: list[Any]) -> str:
        non_null_values = [value for value in values if value is not None]
        if not non_null_values:
            return "TEXT"
        if any(isinstance(value, (dict, list)) for value in non_null_values):
            return "JSONB"
        if all(isinstance(value, bool) for value in non_null_values):
            return "BOOLEAN"
        if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null_values):
            return "INTEGER"
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null_values):
            return "DOUBLE PRECISION"
        return "TEXT"

    def _coerce_dynamic_value(self, value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return Jsonb(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _get_dynamic_columns(self, cur, table_name: str) -> dict[str, str]:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return {row["column_name"]: row["data_type"] for row in cur.fetchall()}

    def list_dynamic_tables(self) -> list[DynamicTableItem]:
        with self._connect() as conn, conn.cursor() as cur:
            table_names = sorted(self._dynamic_table_names(cur))
            items: list[DynamicTableItem] = []
            for table_name in table_names:
                columns = list(self._get_dynamic_columns(cur, table_name).keys())
                cur.execute(
                    sql.SQL("SELECT COUNT(*)::int AS row_count FROM {}").format(sql.Identifier(table_name))
                )
                row_count = cur.fetchone()["row_count"]
                items.append(
                    DynamicTableItem(
                        table_name=table_name,
                        row_count=row_count,
                        columns=[column for column in columns if column not in {"id", "ingested_at"}],
                    )
                )
            return items

    def get_dynamic_table_preview(self, table_name: str, limit: int = 20) -> DynamicTablePreview:
        normalized_table_name = self._normalize_identifier(table_name)
        if not normalized_table_name:
            raise ValueError("Invalid table name")
        with self._connect() as conn, conn.cursor() as cur:
            if normalized_table_name not in self._dynamic_table_names(cur):
                raise ValueError(f"Dynamic table '{normalized_table_name}' was not found")
            columns = list(self._get_dynamic_columns(cur, normalized_table_name).keys())
            cur.execute(
                sql.SQL("SELECT * FROM {} ORDER BY id DESC LIMIT %s").format(sql.Identifier(normalized_table_name)),
                (limit,),
            )
            rows = cur.fetchall()
        return DynamicTablePreview(table_name=normalized_table_name, columns=columns, rows=rows)

    def ingest_dynamic_data(self, payload: DynamicDataIngest):
        if not payload.data:
            return {"message": "No data to ingest"}

        table_name = self._normalize_identifier(payload.table_name)
        if not table_name:
            return {"error": "Invalid table name. Use letters, numbers, spaces or underscores."}

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    table_exists = table_name in self._dynamic_table_names(cur)
                    if not table_exists and not payload.create_if_missing:
                        return {"error": f"Table '{table_name}' does not exist."}

                    column_samples: dict[str, list[Any]] = {}
                    for row in payload.data:
                        if not isinstance(row, dict):
                            continue
                        for key, value in row.items():
                            col_name = self._normalize_identifier(key)
                            if not col_name or col_name in {"id", "ingested_at"}:
                                continue
                            column_samples.setdefault(col_name, []).append(value)

                    if not column_samples:
                        return {"error": "No valid columns found in data"}

                    inferred_columns = {
                        col_name: self._infer_dynamic_column_type(values)
                        for col_name, values in column_samples.items()
                    }

                    if not table_exists:
                        create_columns = [
                            sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
                            for col_name, col_type in inferred_columns.items()
                        ]
                        cur.execute(
                            sql.SQL(
                                "CREATE TABLE {} (id SERIAL PRIMARY KEY, ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), {})"
                            ).format(sql.Identifier(table_name), sql.SQL(", ").join(create_columns))
                        )
                    else:
                        existing_columns = self._get_dynamic_columns(cur, table_name)
                        for col_name, col_type in inferred_columns.items():
                            if col_name not in existing_columns:
                                cur.execute(
                                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                                        sql.Identifier(table_name),
                                        sql.Identifier(col_name),
                                        sql.SQL(col_type),
                                    )
                                )

                    for row in payload.data:
                        valid_row = {
                            self._normalize_identifier(key): self._coerce_dynamic_value(value)
                            for key, value in row.items()
                            if self._normalize_identifier(key) and self._normalize_identifier(key) not in {"id", "ingested_at"}
                        }
                        valid_row = {key: value for key, value in valid_row.items() if key}
                        if not valid_row:
                            continue
                        keys = list(valid_row.keys())
                        cur.execute(
                            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                                sql.Identifier(table_name),
                                sql.SQL(", ").join(sql.Identifier(key) for key in keys),
                                sql.SQL(", ").join(sql.Placeholder(key) for key in keys),
                            ),
                            valid_row,
                        )
                conn.commit()
            preview = self.get_dynamic_table_preview(table_name, limit=min(5, len(payload.data)))
            return {
                "message": f"Successfully ingested {len(payload.data)} rows into {table_name}",
                "table_name": table_name,
                "columns": preview.columns,
                "preview": preview.rows,
            }
        except Exception as e:
            return {"error": f"Database error: {str(e)}"}

    def _extract_json_payload(self, text: str) -> Any:
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            match = re.search(r"(\{.*\}|\[.*\])", stripped, re.DOTALL)
            if not match:
                raise ValueError("The AI response did not contain valid JSON.")
            return json.loads(match.group(1))

    async def extract_json_from_image(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
    ) -> ImageJsonExtractionResponse:
        if not self.settings.openrouter_api_key:
            raise ValueError("OpenRouter API key not configured")
        if not image_bytes:
            raise ValueError("No image data received")

        data_url = f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('utf-8')}"
        system_prompt = (
            "You extract structured sports data from images. "
            "Return valid JSON only. Do not wrap the response in markdown. "
            "If some fields are uncertain, include them with null values or add a short 'notes' field."
        )
        fallback_models = [
            model
            for model in self.settings.openrouter_vision_fallback_models_list
            if model and model != self.settings.openrouter_vision_model
        ]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.settings.openrouter_api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": str(self.settings.frontend_origin),
                    "X-Title": self.settings.app_name,
                },
                json={
                    "model": self.settings.openrouter_vision_model,
                    "models": fallback_models,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": prompt},
                                {"type": "image_url", "image_url": {"url": data_url}},
                            ],
                        },
                    ],
                },
                timeout=90.0,
            )
            if response.status_code != 200:
                raise ValueError(f"OpenRouter API error: {response.text}")

        result = response.json()
        raw_response = result["choices"][0]["message"]["content"]
        parsed_json = self._extract_json_payload(raw_response)
        return ImageJsonExtractionResponse(
            json_data=parsed_json,
            raw_response=raw_response,
            model=self.settings.openrouter_vision_model,
        )

    async def analyze_with_ai(self, payload: AIAnalysisRequest):
        if not self.settings.openrouter_api_key:
            return {"error": "OpenRouter API key not configured"}

        table_context = None
        table_name = None
        if payload.table_name:
            try:
                preview = self.get_dynamic_table_preview(payload.table_name, payload.row_limit)
                table_name = preview.table_name
                table_context = {
                    "table_name": preview.table_name,
                    "columns": preview.columns,
                    "rows": preview.rows,
                }
            except Exception as exc:
                return {"error": f"Unable to load table context: {exc}"}

        system_prompt = (
            "You are an expert Malawian sports data analyst working on football, athletics, and league datasets. "
            "Write in clear, energetic English. Ground every claim in the supplied data, identify standout performers, "
            "patterns, anomalies, and practical takeaways, and avoid inventing facts that are not supported by the data."
        )
        user_payload = {
            "task": payload.prompt,
            "table_context": table_context,
            "manual_context": payload.data_context,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.settings.openrouter_api_key}",
                        "Content-Type": "application/json",
                        "HTTP-Referer": str(self.settings.frontend_origin),
                        "X-Title": self.settings.app_name,
                    },
                    json={
                        "model": self.settings.openrouter_model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": json.dumps(user_payload, indent=2, default=str)},
                        ],
                    },
                    timeout=60.0,
                )
                if response.status_code != 200:
                    return {"error": f"OpenRouter API error: {response.text}"}

                result = response.json()
                return {
                    "analysis": result["choices"][0]["message"]["content"],
                    "table_name": table_name,
                    "model": self.settings.openrouter_model,
                }
        except Exception as e:
            return {"error": f"AI analysis error: {str(e)}"}


postgres_service = PostgresService()


def get_connection_status() -> dict[str, str | bool]:
    configured = bool(get_settings().database_url)
    return {
        "configured": configured,
        "connected": postgres_service.connected,
        "database": "postgresql",
    }


def try_connect() -> None:
    try:
        postgres_service.initialize()
    except Exception:
        postgres_service.connected = False


def close_connection() -> None:
    with suppress(Exception):
        postgres_service.connected = False
