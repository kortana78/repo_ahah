from typing import Any
from enum import Enum
from pydantic import BaseModel, Field

class StatItem(BaseModel):
    label: str
    value: str

class HighlightCard(BaseModel):
    title: str
    body: str
    icon_key: str

class FixtureItem(BaseModel):
    id: int
    week_label: str
    match_date: str | None
    home: str
    away: str
    score: str

class TeamItem(BaseModel):
    id: int
    name: str
    city: str
    icon_text: str

class CompetitionItem(BaseModel):
    id: int
    name: str
    season_year: int
    country: str | None = None
    current_season: str | None = None
    logo: str | None = None

class StandingItem(BaseModel):
    rank: int
    team: str
    played: int
    wins: int
    draws: int
    losses: int
    gf: int
    ga: int
    gd: int
    points: int
    status: str
    form: str | None = None

class PlayerStatItem(BaseModel):
    player: str
    team: str
    value: int
    matches_played: int | None = None

class AgentItem(BaseModel):
    id: int
    name: str
    agency_name: str | None = None
    fifa_license_number: str | None = None
    whatsapp_number: str | None = None
    email: str | None = None
    linkedin_profile: str | None = None

class AgentUpsert(BaseModel):
    name: str
    agency_name: str | None = None
    fifa_license_number: str | None = None
    whatsapp_number: str | None = None
    email: str | None = None
    linkedin_profile: str | None = None

class PlayerProfile(BaseModel):
    id: int
    name: str
    team_id: int
    team_name: str
    position: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    preferred_foot: str | None = None
    date_of_birth: str | None = None
    citizenship: str | None = None
    contract_expires: str | None = None
    joining_date: str | None = None
    is_transfer_listed: bool = False
    loan_status: bool = False
    market_value: float = 0.0
    currency: str = "MWK"
    agent_id: int | None = None
    agent_name: str | None = None

class PlayerProfileUpsert(BaseModel):
    name: str
    team_id: int
    position: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    preferred_foot: str | None = None
    date_of_birth: str | None = None
    citizenship: str | None = None
    contract_expires: str | None = None
    joining_date: str | None = None
    is_transfer_listed: bool = False
    loan_status: bool = False
    agent_id: int | None = None

class UserRole(str, Enum):
    ADMIN = "admin"
    SCOUT = "scout"
    AGENT = "agent"
    GUEST = "guest"

class User(BaseModel):
    id: int
    username: str
    email: str
    role: UserRole
    is_active: bool = True

class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    role: UserRole = UserRole.GUEST

class Token(BaseModel):
    access_token: str
    token_type: str

class PlayerTransferCard(BaseModel):
    player: PlayerProfile
    stats: list[PlayerStatItem]
    valuation_history: list[dict[str, Any]]
    highlights: list[str] = Field(default_factory=list)

class SourceAssetItem(BaseModel):
    id: int
    filename: str
    category: str
    label: str
    source_url: str | None = None
    source_domain: str | None = None
    source_kind: str | None = None
    image_url: str | None = None
    thumbnail_url: str | None = None
    mime_type: str | None = None
    notes: str | None = None
    tags: list[str] = Field(default_factory=list)
    scrape_status: str | None = None
    external_id: str | None = None
    competition_id: int | None = None
    is_manual: bool = True
    is_premium: bool = False
    price: float = 0.0
    created_at: str | None = None
    updated_at: str | None = None

class MarketValuationItem(BaseModel):
    player_id: int
    player_name: str
    team_name: str
    position: str | None = None
    market_value: float
    currency: str = "MWK"
    is_premium: bool = False
    valuation_date: str | None = None

class RepositoryOverview(BaseModel):
    brand: str
    tagline: str
    hero_badge: str
    hero_title: str
    hero_body: str
    active_competition: CompetitionItem | None = None
    stats: list[StatItem]
    highlights: list[HighlightCard]
    fixtures: list[FixtureItem]
    teams: list[TeamItem]
    competitions: list[CompetitionItem]
    standings: list[StandingItem]
    top_scorers: list[PlayerStatItem]
    assist_leaders: list[PlayerStatItem]
    source_assets: list[SourceAssetItem] = Field(default_factory=list)
    market_valuations: list[MarketValuationItem] = Field(default_factory=list)

class ImportHighlight(BaseModel):
    title: str
    body: str
    icon_key: str
    sort_order: int = Field(ge=0, default=0)
    competition_name: str | None = None
    competition_season_year: int | None = None

class ImportTeam(BaseModel):
    name: str
    short_name: str | None = None
    city: str | None = None
    icon_text: str | None = None

class ImportStanding(BaseModel):
    week_label: str
    snapshot_date: str
    rank: int
    team: str
    played: int
    wins: int
    draws: int
    losses: int
    gf: int
    ga: int
    gd: int
    points: int
    status: str

class ImportMatchEvent(BaseModel):
    team: str
    player: str
    minute: str
    event_type: str
    is_penalty: bool = False
    is_own_goal: bool = False

class ImportMatch(BaseModel):
    week_no: int | None = None
    week_label: str
    match_date: str | None = None
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    source_image: str | None = None
    events: list[ImportMatchEvent] = Field(default_factory=list)

class ImportPlayerStat(BaseModel):
    player: str
    team: str
    stat_type: str
    value: int
    matches_played: int | None = None
    season_year: int | None = None

class ImportCompetition(BaseModel):
    name: str
    season_year: int
    country: str | None = None
    current_season: str | None = None
    logo: str | None = None
    teams: list[ImportTeam] = Field(default_factory=list)
    standings: list[ImportStanding] = Field(default_factory=list)
    matches: list[ImportMatch] = Field(default_factory=list)
    player_stats: list[ImportPlayerStat] = Field(default_factory=list)
    highlights: list[ImportHighlight] = Field(default_factory=list)

class ImportSourceAsset(BaseModel):
    filename: str
    category: str
    label: str
    source_url: str | None = None
    source_kind: str | None = None
    image_url: str | None = None
    thumbnail_url: str | None = None
    mime_type: str | None = None
    notes: str | None = None
    tags: list[str] = Field(default_factory=list)
    scrape_status: str | None = None
    external_id: str | None = None
    competition_id: int | None = None
    is_manual: bool = True
    is_premium: bool = False
    price: float = 0.0

class RepositoryMeta(BaseModel):
    brand: str | None = None
    tagline: str | None = None
    hero_badge: str | None = None
    hero_title: str | None = None
    hero_body: str | None = None

class ImportPayload(BaseModel):
    repository: RepositoryMeta | None = None
    competitions: list[ImportCompetition] = Field(default_factory=list)
    highlights: list[ImportHighlight] = Field(default_factory=list)
    source_assets: list[ImportSourceAsset] = Field(default_factory=list)

class CompetitionUpsert(BaseModel):
    name: str
    season_year: int
    country: str | None = None
    current_season: str | None = None
    logo: str | None = None

class TeamUpsert(BaseModel):
    name: str
    short_name: str | None = None
    city: str | None = None
    icon_text: str | None = None

class HighlightUpsert(BaseModel):
    title: str
    body: str
    icon_key: str
    sort_order: int = Field(ge=0, default=0)
    competition_id: int | None = None

class MatchEventInput(BaseModel):
    team: str
    player: str
    minute: str
    event_type: str
    is_penalty: bool = False
    is_own_goal: bool = False

class MatchUpsert(BaseModel):
    competition_id: int
    week_no: int | None = None
    week_label: str
    match_date: str | None = None
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    source_image: str | None = None
    source_asset_id: int | None = None
    image_url: str | None = None
    notes: str | None = None
    events: list[MatchEventInput] = Field(default_factory=list)

class ManagedMatch(BaseModel):
    id: int
    competition_id: int
    competition_name: str
    week_no: int | None = None
    week_label: str
    match_date: str | None = None
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    source_image: str
    source_asset_id: int | None = None
    image_url: str | None = None
    notes: str | None = None
    events: list[MatchEventInput] = Field(default_factory=list)

class SourceAssetUpsert(BaseModel):
    filename: str
    category: str
    label: str
    source_url: str | None = None
    source_kind: str | None = None
    image_url: str | None = None
    thumbnail_url: str | None = None
    mime_type: str | None = None
    notes: str | None = None
    tags: list[str] = Field(default_factory=list)
    scrape_status: str | None = None
    external_id: str | None = None
    competition_id: int | None = None
    is_manual: bool = True
    is_premium: bool = False
    price: float = 0.0

class PlayerValuationUpdate(BaseModel):
    player_id: int
    market_value: float
    currency: str = "MWK"
    is_premium: bool = False

class SourceIngestionRequest(BaseModel):
    url: str
    label: str | None = None
    category: str | None = None
    competition_id: int | None = None
    notes: str | None = None
    tags: list[str] = Field(default_factory=list)
    fetch_preview: bool = True

class SourceIngestionResult(BaseModel):
    asset: SourceAssetItem
    categorization: dict[str, Any]
    fetched: bool
    warnings: list[str] = Field(default_factory=list)

class CommercialCompetitionFeed(BaseModel):
    competition: CompetitionItem
    latest_standings: list[StandingItem] = Field(default_factory=list)
    recent_matches: list[ManagedMatch] = Field(default_factory=list)
    source_assets: list[SourceAssetItem] = Field(default_factory=list)

class CommercialFeed(BaseModel):
    repository: RepositoryMeta
    competitions: list[CommercialCompetitionFeed] = Field(default_factory=list)

class DynamicDataIngest(BaseModel):
    table_name: str
    create_if_missing: bool = True
    data: list[dict[str, Any]]

class DynamicTableItem(BaseModel):
    table_name: str
    row_count: int
    columns: list[str] = Field(default_factory=list)

class DynamicTablePreview(BaseModel):
    table_name: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)

class AIAnalysisRequest(BaseModel):
    prompt: str
    table_name: str | None = None
    row_limit: int = Field(default=20, ge=1, le=100)
    data_context: Any | None = None
