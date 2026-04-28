import os
import shutil
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, File, Form, Query, UploadFile, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.config import get_settings
from app.db import close_connection, get_connection_status, postgres_service, try_connect
from app.schemas import (
    AgentUpsert,
    CompetitionUpsert,
    DynamicTableItem,
    DynamicTablePreview,
    HighlightUpsert,
    ImportPayload,
    MatchUpsert,
    PlayerProfileUpsert,
    PlayerValuationUpdate,
    RepositoryMeta,
    SourceAssetUpsert,
    SourceIngestionRequest,
    TeamUpsert,
    UserCreate,
    UserRole,
    Token,
    User,
    DynamicDataIngest,
    ImageJsonExtractionResponse,
    AIAnalysisRequest,
)

settings = get_settings()
os.makedirs(settings.uploads_dir, exist_ok=True)
cors_origins = [
    settings.frontend_origin,
    *settings.frontend_origins_list,
    "http://127.0.0.1:5173",
    "http://localhost:5173",
]
cors_origins = list(dict.fromkeys(origin for origin in cors_origins if origin))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user_dict = postgres_service.get_user_by_username(username)
    if user_dict is None:
        raise credentials_exception
    return User(**user_dict)

def check_role(user: User, required_roles: list[UserRole]):
    if user.role not in [role.value for role in required_roles]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have enough permissions",
        )

@asynccontextmanager
async def lifespan(_: FastAPI):
    try_connect()
    yield
    close_connection()


app = FastAPI(title=settings.app_name, lifespan=lifespan)

# Mount static files for uploads
app.mount("/uploads", StaticFiles(directory=settings.uploads_dir), name="uploads")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=r"^https://([a-z0-9-]+\.)?vercel\.app$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str | bool]:
    status = get_connection_status()
    return {
        "status": "ok",
        "configured": status["configured"],
        "connected": status["connected"],
        "database": status["database"],
    }

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user_dict = postgres_service.get_user_by_username(form_data.username)
    if not user_dict or not pwd_context.verify(form_data.password, user_dict["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"sub": user_dict["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/register", response_model=User)
def register_user(payload: UserCreate = Body(...)):
    hashed_password = pwd_context.hash(payload.password)
    return postgres_service.create_user(payload.username, payload.email, hashed_password, payload.role.value)

@app.get("/api/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user


@app.get("/api/repository/overview")
def repository_overview(competition_id: int | None = Query(default=None)):
    return postgres_service.get_overview(competition_id)


@app.post("/api/repository/import")
def repository_import(payload: ImportPayload = Body(...)):
    return postgres_service.import_payload(payload)


@app.get("/api/repository/meta")
def repository_meta():
    return postgres_service.get_repository_meta()


@app.put("/api/repository/meta")
def repository_meta_update(payload: RepositoryMeta = Body(...)):
    return postgres_service.save_repository_meta(payload)


@app.get("/api/admin/competitions")
def admin_list_competitions():
    return postgres_service.list_competitions()


@app.post("/api/admin/competitions")
def admin_create_competition(payload: CompetitionUpsert = Body(...)):
    return postgres_service.save_competition(payload)


@app.put("/api/admin/competitions/{competition_id}")
def admin_update_competition(competition_id: int, payload: CompetitionUpsert = Body(...)):
    return postgres_service.save_competition(payload, competition_id)


@app.get("/api/admin/teams")
def admin_list_teams():
    return postgres_service.list_teams()


@app.post("/api/admin/teams")
def admin_create_team(payload: TeamUpsert = Body(...)):
    return postgres_service.save_team(payload)


@app.put("/api/admin/teams/{team_id}")
def admin_update_team(team_id: int, payload: TeamUpsert = Body(...)):
    return postgres_service.save_team(payload, team_id)


@app.get("/api/admin/highlights")
def admin_list_highlights(competition_id: int | None = Query(default=None)):
    return postgres_service.list_highlights(competition_id)


@app.post("/api/admin/highlights")
def admin_create_highlight(payload: HighlightUpsert = Body(...)):
    return postgres_service.save_highlight(payload)


@app.put("/api/admin/highlights/{highlight_id}")
def admin_update_highlight(highlight_id: int, payload: HighlightUpsert = Body(...)):
    return postgres_service.save_highlight(payload, highlight_id)


@app.get("/api/admin/source-assets")
def admin_list_source_assets(
    competition_id: int | None = Query(default=None),
    category: str | None = Query(default=None),
    domain: str | None = Query(default=None),
):
    return postgres_service.list_source_assets(competition_id, category, domain)


@app.post("/api/admin/source-assets")
def admin_create_source_asset(payload: SourceAssetUpsert = Body(...)):
    return postgres_service.save_source_asset(payload)


@app.put("/api/admin/source-assets/{asset_id}")
def admin_update_source_asset(asset_id: int, payload: SourceAssetUpsert = Body(...)):
    return postgres_service.save_source_asset(payload, asset_id)


@app.post("/api/admin/source-ingestions/url")
def admin_ingest_source_url(payload: SourceIngestionRequest = Body(...)):
    return postgres_service.ingest_source_url(payload)


@app.get("/api/admin/matches")
def admin_list_matches(competition_id: int | None = Query(default=None)):
    return postgres_service.list_matches(competition_id)


@app.post("/api/admin/matches")
def admin_create_match(payload: MatchUpsert = Body(...)):
    return postgres_service.save_match(payload)


@app.put("/api/admin/matches/{match_id}")
def admin_update_match(match_id: int, payload: MatchUpsert = Body(...)):
    return postgres_service.save_match(payload, match_id)


@app.post("/api/admin/upload-image")
async def admin_upload_image(file: UploadFile = File(...)):
    file_path = settings.uploads_dir / file.filename
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Return the URL to the uploaded file
    # In a real app, we'd use the request URL base
    file_url = f"http://127.0.0.1:8000/uploads/{file.filename}"
    return {"url": file_url, "filename": file.filename}


@app.get("/api/admin/market-valuations")
def admin_list_market_valuations():
    return postgres_service.list_market_valuations()


@app.put("/api/admin/market-valuations")
def admin_update_market_valuation(payload: PlayerValuationUpdate = Body(...)):
    return postgres_service.update_player_valuation(payload)


@app.get("/api/admin/agents")
def admin_list_agents(current_user: User = Depends(get_current_user)):
    check_role(current_user, [UserRole.ADMIN, UserRole.AGENT])
    return postgres_service.list_agents()


@app.post("/api/admin/agents")
def admin_create_agent(payload: AgentUpsert = Body(...), current_user: User = Depends(get_current_user)):
    check_role(current_user, [UserRole.ADMIN])
    return postgres_service.save_agent(payload)


@app.put("/api/admin/agents/{agent_id}")
def admin_update_agent(agent_id: int, payload: AgentUpsert = Body(...), current_user: User = Depends(get_current_user)):
    check_role(current_user, [UserRole.ADMIN])
    return postgres_service.save_agent(payload, agent_id)


@app.get("/api/players/{player_id}")
def get_player_profile(player_id: int, current_user: User = Depends(get_current_user)):
    profile = postgres_service.get_player_profile(player_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Player not found")
    
    # Check for premium visibility
    if profile.is_premium and current_user.role not in [UserRole.ADMIN, UserRole.SCOUT]:
        profile.market_value = 0 # Hide premium value or raise error
        # profile.market_value = -1 # Or some indicator
    
    return profile


@app.post("/api/admin/players")
def admin_create_player(payload: PlayerProfileUpsert = Body(...), current_user: User = Depends(get_current_user)):
    check_role(current_user, [UserRole.ADMIN])
    return postgres_service.save_player_profile(payload)


@app.put("/api/admin/players/{player_id}")
def admin_update_player(player_id: int, payload: PlayerProfileUpsert = Body(...), current_user: User = Depends(get_current_user)):
    check_role(current_user, [UserRole.ADMIN])
    return postgres_service.save_player_profile(payload, player_id)


@app.get("/api/players/{player_id}/transfer-card")
def get_player_transfer_card(player_id: int, current_user: User = Depends(get_current_user)):
    # Only scouts and admins can see full transfer cards for premium players
    card = postgres_service.get_player_transfer_card(player_id)
    if not card:
        raise HTTPException(status_code=404, detail="Player not found")
    
    if card.player.is_premium and current_user.role not in [UserRole.ADMIN, UserRole.SCOUT]:
        raise HTTPException(status_code=403, detail="Premium content restricted to Scouts")
        
    return card


@app.get("/api/commercial/feed")
def commercial_feed():
    return postgres_service.get_commercial_feed()


@app.get("/api/commercial/competitions/{competition_id}/matches")
def commercial_competition_matches(competition_id: int):
    return postgres_service.list_matches(competition_id)


@app.get("/api/commercial/source-assets")
def commercial_source_assets(
    competition_id: int | None = Query(default=None),
    category: str | None = Query(default=None),
    domain: str | None = Query(default=None),
):
    return postgres_service.list_source_assets(competition_id, category, domain)


@app.post("/api/admin/ingest-dynamic")
def admin_ingest_dynamic(payload: DynamicDataIngest = Body(...)):
    return postgres_service.ingest_dynamic_data(payload)

@app.get("/api/admin/dynamic-tables", response_model=list[DynamicTableItem])
def admin_list_dynamic_tables():
    return postgres_service.list_dynamic_tables()


@app.get("/api/admin/dynamic-tables/{table_name}", response_model=DynamicTablePreview)
def admin_dynamic_table_preview(table_name: str, limit: int = Query(default=20, ge=1, le=100)):
    return postgres_service.get_dynamic_table_preview(table_name, limit)


@app.post("/api/ai/extract-json-from-image", response_model=ImageJsonExtractionResponse)
async def ai_extract_json_from_image(
    file: UploadFile = File(...),
    prompt: str = Form(default="Extract the sports data in this image into valid JSON only."),
):
    image_bytes = await file.read()
    try:
        return await postgres_service.extract_json_from_image(
            image_bytes=image_bytes,
            mime_type=file.content_type or "image/jpeg",
            prompt=prompt,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/ai/analyze")
async def ai_analyze(payload: AIAnalysisRequest = Body(...)):
    return await postgres_service.analyze_with_ai(payload)
