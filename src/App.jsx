import { useEffect, useMemo, useState, useCallback } from 'react'
import Admin from './Admin'

const navItems = [
  { label: 'Management', icon: 'grid', href: '#admin' },
  { label: 'Marketplace', icon: 'trophy', href: '#marketplace' },
  { label: 'Matches', icon: 'ball', href: '#matches' },
  { label: 'Standings', icon: 'shield', href: '#standings' },
  { label: 'Leaders', icon: 'user', href: '#leaders' },
  { label: 'Teams', icon: 'shield', href: '#teams' },
  { label: 'Competitions', icon: 'trophy', href: '#competitions' },
  { label: 'Import JSON', icon: 'grid', href: '#import' },
  { label: 'AI Insights', icon: 'search', href: '#insights' },
]

const fallbackData = {
  brand: 'NyasaSport Repo',
  tagline: 'Multi-Competition Data Repository',
  hero_badge: 'Market Valuation | Premium Scouting',
  hero_title: 'NyasaSport Repository',
  hero_body: 'Professional scouting data, player valuations, and premium match assets for teams.',
  active_competition: null,
  stats: [
    { label: 'Tracked Teams', value: '16' },
    { label: 'Named Players', value: '12' },
    { label: 'Loaded Matches', value: '08' },
    { label: 'Source Assets', value: '10' },
  ],
  highlights: [
    {
      title: 'Scouting Portal',
      body: 'Verified player market values and tactical data tiers for team recruitment.',
      icon_key: 'user',
    },
    {
      title: 'Commercial Assets',
      body: 'High-quality match images and datasets available for commercial licensing.',
      icon_key: 'trophy',
    },
    {
      title: 'Data Reliability',
      body: 'Multi-source verification for all standings and player performance logs.',
      icon_key: 'shield',
    },
  ],
  fixtures: [],
  teams: [],
  competitions: [
    {
      id: 1,
      name: 'TNM Super League',
      season_year: 2025,
      country: 'Malawi',
      current_season: '2025',
      logo: 'tnm-super-league',
    },
  ],
  standings: [],
  top_scorers: [],
  assist_leaders: [],
  source_assets: [],
  market_valuations: [],
}

function Icon({ kind }) {
  const paths = {
    grid: 'M4 4h6v6H4zM14 4h6v6h-6zM4 14h6v6H4zM14 14h6v6h-6z',
    trophy:
      'M8 4h8v3h3a1 1 0 0 1 1 1v1a5 5 0 0 1-5 5h-1a4 4 0 0 1-2 2.7V20h4v2H8v-2h4v-2.3A4 4 0 0 1 10 15h-1a5 5 0 0 1-5-5V9a1 1 0 0 1 1-1h3V4zm-2 6a3 3 0 0 0 3 3h1V10H6zm12 0h-4v3h1a3 3 0 0 0 3-3z',
    shield: 'M12 3l7 3v5c0 5-3.5 8.7-7 10-3.5-1.3-7-5-7-10V6l7-3z',
    user: 'M12 12a4 4 0 1 0-4-4 4 4 0 0 0 4 4zm0 2c-4.4 0-8 2.2-8 5v1h16v-1c0-2.8-3.6-5-8-5z',
    ball: 'M12 3l3 2 4 .5 2.5 3-1 4 1 4-2.5 3-4 .5-3 2-3-2-4-.5-2.5-3 1-4-1-4 2.5-3 4-.5z',
    table:
      'M4 5h16a1 1 0 0 1 1 1v12a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1zm0 4h16M9 5v13M15 5v13',
    goal: 'M4 7h10v10H4zM14 10l6-3v10l-6-3',
    assist:
      'M7 13l3 3 7-7M5 19h14a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2z',
    search: 'M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z',
  }

  return (
    <svg className="icon" viewBox="0 0 24 24" aria-hidden="true">
      <path d={paths[kind] ?? paths.grid} />
    </svg>
  )
}

function Badge({ children }) {
  return <span className="badge">{children}</span>
}

function Button({ children, variant = 'primary', ...props }) {
  return (
    <button className={`btn btn--${variant}`} type="button" {...props}>
      {children}
    </button>
  )
}

function TeamMark({ text }) {
  return <span className="team-mark">{text}</span>
}

export default function App() {
  const [data, setData] = useState(fallbackData)
  const [status, setStatus] = useState('loading')
  const [activeCompetitionId, setActiveCompetitionId] = useState(null)
  const [importStatus, setImportStatus] = useState('idle')
  const [importError, setImportError] = useState('')
  const [importFile, setImportFile] = useState(null)
  const [importText, setImportText] = useState('')
  const [dynamicTableName, setDynamicTableName] = useState('')
  const [dynamicTables, setDynamicTables] = useState([])
  const [dynamicStatus, setDynamicStatus] = useState('idle')
  const [dynamicError, setDynamicError] = useState('')
  const [dynamicPreview, setDynamicPreview] = useState([])
  const [analysisPrompt, setAnalysisPrompt] = useState(
    'Analyse this Malawian sports data and highlight the strongest trends, standout performers, and practical insights.'
  )
  const [analysisTable, setAnalysisTable] = useState('')
  const [analysisContext, setAnalysisContext] = useState('')
  const [analysisStatus, setAnalysisStatus] = useState('idle')
  const [analysisError, setAnalysisError] = useState('')
  const [analysisResponse, setAnalysisResponse] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  
  // Auth and Player Profile state
  const [user, setUser] = useState(null)
  const [token, setToken] = useState(localStorage.getItem('token'))
  const [showLogin, setShowLogin] = useState(false)
  const [loginForm, setLoginForm] = useState({ username: '', password: '' })
  const [selectedPlayerId, setSelectedPlayerId] = useState(null)
  const [transferCard, setTransferCard] = useState(null)

  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://127.0.0.1:8000'

  const loadOverview = useCallback(async (competitionId = null) => {
    try {
      const url = new URL(`${apiBaseUrl}/api/repository/overview`)
      const compId = competitionId || activeCompetitionId
      if (compId) {
        url.searchParams.set('competition_id', compId)
      }
      const response = await fetch(url.toString())

      if (!response.ok) {
        throw new Error('Unable to load repository overview')
      }

      const payload = await response.json()
      setData(payload)
      if (!compId && payload?.active_competition?.id) {
        setActiveCompetitionId(payload.active_competition.id)
      }
      setStatus('ready')
    } catch (error) {
      setStatus('offline')
    }
  }, [activeCompetitionId, apiBaseUrl])

  const loadDynamicTables = useCallback(async () => {
    try {
      const response = await fetch(`${apiBaseUrl}/api/admin/dynamic-tables`)
      if (!response.ok) {
        throw new Error('Unable to load custom data tables')
      }
      const payload = await response.json()
      setDynamicTables(payload)
      setAnalysisTable((current) => current || payload[0]?.table_name || '')
    } catch (error) {
      setDynamicTables([])
    }
  }, [apiBaseUrl])

  useEffect(() => {
    loadOverview()
    loadDynamicTables()
    if (token) fetchMe()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function fetchMe() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/me`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      if (res.ok) setUser(await res.json())
      else {
        setToken(null)
        localStorage.removeItem('token')
      }
    } catch (e) { console.error(e) }
  }

  async function handleLogin(e) {
    e.preventDefault()
    try {
      const formData = new URLSearchParams()
      formData.append('username', loginForm.username)
      formData.append('password', loginForm.password)
      const res = await fetch(`${apiBaseUrl}/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: formData
      })
      if (!res.ok) throw new Error('Login failed')
      const { access_token } = await res.json()
      setToken(access_token)
      localStorage.setItem('token', access_token)
      setShowLogin(false)
      fetchMe()
    } catch (e) { alert(e.message) }
  }

  async function loadTransferCard(playerId) {
    if (!token) {
      setShowLogin(true)
      return
    }
    try {
      const res = await fetch(`${apiBaseUrl}/api/players/${playerId}/transfer-card`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      if (res.ok) {
        setTransferCard(await res.json())
        setSelectedPlayerId(playerId)
      } else if (res.status === 403) {
        alert("This is a Premium Scout feature. Please upgrade your account.")
      } else {
        throw new Error('Failed to load transfer card')
      }
    } catch (e) { alert(e.message) }
  }

  const activeCompetition = useMemo(() => {
    if (!data?.competitions?.length) return null
    return (
      data.competitions.find((item) => item.id === activeCompetitionId) ??
      data.active_competition ??
      data.competitions[0]
    )
  }, [data, activeCompetitionId])

  const filteredMatches = useMemo(() => {
    if (!searchTerm) return data.fixtures
    const term = searchTerm.toLowerCase()
    return data.fixtures.filter(f => 
      f.home.toLowerCase().includes(term) || 
      f.away.toLowerCase().includes(term) ||
      f.week_label.toLowerCase().includes(term)
    )
  }, [data.fixtures, searchTerm])

  const filteredStandings = useMemo(() => {
    if (!searchTerm) return data.standings
    const term = searchTerm.toLowerCase()
    return data.standings.filter(s => s.team.toLowerCase().includes(term))
  }, [data.standings, searchTerm])

  const filteredTeams = useMemo(() => {
    if (!searchTerm) return data.teams
    const term = searchTerm.toLowerCase()
    return data.teams.filter(t => t.name.toLowerCase().includes(term) || t.city.toLowerCase().includes(term))
  }, [data.teams, searchTerm])

  async function readJsonInput() {
    if (importText.trim()) {
      return JSON.parse(importText)
    }
    if (importFile) {
      const text = await importFile.text()
      return JSON.parse(text)
    }
    throw new Error('Paste JSON text or choose a JSON file first.')
  }

  async function handleImport() {
    setImportStatus('loading')
    setImportError('')
    try {
      const payload = await readJsonInput()
      const response = await fetch(`${apiBaseUrl}/api/repository/import`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!response.ok) {
        throw new Error('Import failed. Check the JSON structure.')
      }
      await response.json()
      setImportStatus('success')
      loadOverview()
      setImportFile(null)
    } catch (error) {
      setImportStatus('error')
      setImportError(error.message ?? 'Import failed.')
    }
  }

  async function handleDynamicIngest() {
    setDynamicStatus('loading')
    setDynamicError('')
    setDynamicPreview([])
    try {
      if (!dynamicTableName.trim()) {
        throw new Error('Enter a table name first.')
      }
      const parsed = await readJsonInput()
      const data = Array.isArray(parsed) ? parsed : [parsed]
      const response = await fetch(`${apiBaseUrl}/api/admin/ingest-dynamic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: dynamicTableName,
          create_if_missing: true,
          data,
        }),
      })
      const result = await response.json()
      if (result.error) {
        throw new Error(result.error)
      }
      setDynamicStatus('success')
      setDynamicPreview(result.preview ?? [])
      setAnalysisTable(result.table_name ?? dynamicTableName)
      loadDynamicTables()
    } catch (error) {
      setDynamicStatus('error')
      setDynamicError(error.message ?? 'Dynamic ingest failed.')
    }
  }

  async function handleAnalyze() {
    setAnalysisStatus('loading')
    setAnalysisError('')
    setAnalysisResponse('')
    try {
      const parsedContext = analysisContext.trim() ? JSON.parse(analysisContext) : null
      const response = await fetch(`${apiBaseUrl}/api/ai/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prompt: analysisPrompt,
          table_name: analysisTable || null,
          row_limit: 20,
          data_context: parsedContext,
        }),
      })
      const result = await response.json()
      if (result.error) {
        throw new Error(result.error)
      }
      setAnalysisResponse(result.analysis ?? '')
      setAnalysisStatus('success')
    } catch (error) {
      setAnalysisStatus('error')
      setAnalysisError(error.message ?? 'AI analysis failed.')
    }
  }

  return (
    <div className="pl-layout">
      <aside className="side">
        <div className="side__brand">
          <div className="crest">NS</div>
          <div>
            <h1>{data.brand}</h1>
            <p>{data.tagline}</p>
          </div>
        </div>

        <div className="side__search">
          <Icon kind="search" />
          <input 
            type="text" 
            placeholder="Search teams, matches..." 
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>

        <nav className="side__nav">
          {navItems.map((item) => (
            <a
              key={item.label}
              className={`side__link`}
              href={item.href}
            >
              <Icon kind={item.icon} />
              {item.label}
            </a>
          ))}
          {user ? (
            <div className="side__link" style={{ marginTop: 'auto', borderTop: '1px solid var(--border)' }}>
              <Icon kind="user" />
              <span>{user.username} ({user.role})</span>
              <button 
                onClick={() => { setToken(null); setUser(null); localStorage.removeItem('token'); }}
                style={{ marginLeft: 'auto', background: 'none', border: 'none', color: 'var(--blue)', cursor: 'pointer' }}
              >
                Logout
              </button>
            </div>
          ) : (
            <button className="side__link" style={{ width: '100%', textAlign: 'left', marginTop: 'auto' }} onClick={() => setShowLogin(true)}>
              <Icon kind="user" />
              Login / Scout Access
            </button>
          )}
        </nav>

        <div className="side__footer">
          <Button variant="ghost">Neon Connected</Button>
        </div>
      </aside>

      <main className="pl-main">
        <Admin onRefresh={() => loadOverview()} />

        <section className="content" id="marketplace">
          <div className="content__left">
            <div className="panel">
              <div className="panel__header">
                <h3>Top Market Valuations</h3>
                <span>Scouting Data</span>
              </div>
              <div className="standings">
                <div className="standings__head" style={{ gridTemplateColumns: '1.5fr 1fr 1fr auto' }}>
                  <span>Player</span>
                  <span>Team</span>
                  <span>Value</span>
                  <span>Status</span>
                </div>
                {data.market_valuations.map((p) => (
                  <div 
                    key={p.player_id} 
                    className="standings__row" 
                    style={{ gridTemplateColumns: '1.5fr 1fr 1fr auto', cursor: 'pointer' }}
                    onClick={() => loadTransferCard(p.player_id)}
                  >
                    <span style={{ fontWeight: 600 }}>{p.player_name}</span>
                    <span>{p.team_name}</span>
                    <span style={{ color: 'var(--blue)', fontWeight: 700 }}>
                      {p.currency} {new Intl.NumberFormat().format(p.market_value)}
                    </span>
                    <span>
                      {p.is_premium ? <Badge>Premium</Badge> : <small style={{ color: 'var(--muted)' }}>Public</small>}
                    </span>
                  </div>
                ))}
                {data.market_valuations.length === 0 && (
                  <p className="hero__subtitle" style={{ padding: '20px', textAlign: 'center' }}>
                    No market valuations recorded yet. Add them in Management.
                  </p>
                )}
              </div>
            </div>
          </div>

          <div className="content__right">
            <div className="panel">
              <div className="panel__header">
                <h3>Premium Assets</h3>
                <span>For Sale</span>
              </div>
              <div className="leader-list">
                {data.source_assets.filter(a => a.is_premium).map((asset) => (
                  <div key={asset.id} className="leader">
                    <div>
                      <strong>{asset.label}</strong>
                      <small>{asset.category}</small>
                    </div>
                    <span>MWK {new Intl.NumberFormat().format(asset.price)}</span>
                  </div>
                ))}
                {data.source_assets.filter(a => a.is_premium).length === 0 && (
                  <p className="hero__subtitle">No premium assets available.</p>
                )}
              </div>
              <Button variant="ghost" style={{ marginTop: '12px', width: '100%' }}>View Full Catalog</Button>
            </div>
          </div>
        </section>

        <section className="content" id="matches">
          <div className="content__left">
            <div className="panel">
              <div className="panel__header">
                <h3>Match Logs</h3>
                <span>Extracted Results {searchTerm && `(Filtered: ${filteredMatches.length})`}</span>
              </div>
              <div className="fixture-list">
                {filteredMatches.map((fixture) => (
                  <button key={fixture.id} className="fixture" type="button">
                    <div className="fixture__teams">
                      <span>{fixture.home}</span>
                      <strong>{fixture.score}</strong>
                      <span>{fixture.away}</span>
                    </div>
                    <div className="fixture__meta">
                      <span>{fixture.week_label}</span>
                      <span>{fixture.match_date ?? 'Date not shown on source card'}</span>
                    </div>
                  </button>
                ))}
                {filteredMatches.length === 0 && <p className="hero__subtitle">No matches found matching "{searchTerm}"</p>}
              </div>
            </div>

            <div className="panel" id="standings">
              <div className="panel__header">
                <h3>Standings Snapshot</h3>
                <span>
                  {activeCompetition ? `${activeCompetition.name} ${activeCompetition.season_year}` : ''}
                </span>
              </div>
              <div className="standings">
                <div className="standings__head">
                  <span>#</span>
                  <span>Team</span>
                  <span>P</span>
                  <span>GD</span>
                  <span>Pts</span>
                </div>
                {filteredStandings.map((row) => (
                  <div key={row.rank} className={`standings__row standings__row--${row.status}`}>
                    <span>{row.rank}</span>
                    <span>{row.team}</span>
                    <span>{row.played}</span>
                    <span>{row.gd}</span>
                    <span>{row.points}</span>
                  </div>
                ))}
                {filteredStandings.length === 0 && <p className="hero__subtitle" style={{padding: '12px'}}>No teams found in standings.</p>}
              </div>
            </div>
          </div>

          <div className="content__right" id="leaders">
            <div className="panel">
              <div className="panel__header">
                <h3>Top Scorers</h3>
                <span>Extracted Cards</span>
              </div>
              <div className="leader-list">
                {data.top_scorers.map((item) => (
                  <div key={`${item.player}-${item.team}`} className="leader">
                    <div>
                      <strong>{item.player}</strong>
                      <small>{item.team}</small>
                    </div>
                    <span>{item.value}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="panel">
              <div className="panel__header">
                <h3>Assist Leaders</h3>
                <span>Chart</span>
              </div>
              <div className="leader-list">
                {data.assist_leaders.map((item) => (
                  <div key={`${item.player}-${item.team}`} className="leader">
                    <div>
                      <strong>{item.player}</strong>
                      <small>
                        {item.team}
                        {item.matches_played ? ` - ${item.matches_played} MP` : ''}
                      </small>
                    </div>
                    <span>{item.value}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="panel" id="teams">
              <div className="panel__header">
                <h3>Teams Directory</h3>
                <span>With Icons {searchTerm && `(Filtered: ${filteredTeams.length})`}</span>
              </div>
              <div className="team-list">
                {filteredTeams.map((team) => (
                  <button key={team.id} className="team-pill" type="button">
                    <TeamMark text={team.icon_text} />
                    <div>
                      <span>{team.name}</span>
                      <small>{team.city}</small>
                    </div>
                  </button>
                ))}
                {filteredTeams.length === 0 && <p className="hero__subtitle">No teams found.</p>}
              </div>
            </div>

            <div className="panel" id="competitions">
              <div className="panel__header">
                <h3>Competition</h3>
                <span>Tracked Seasons</span>
              </div>
              <div className="competition-list">
                {data.competitions.map((competition) => (
                  <button
                    key={competition.id}
                    className={`competition-card ${
                      competition.id === activeCompetition?.id ? 'competition-card--active' : ''
                    }`}
                    type="button"
                    onClick={() => setActiveCompetitionId(competition.id)}
                  >
                    <strong>{competition.name}</strong>
                    <span>
                      {competition.season_year}
                      {competition.country ? ` - ${competition.country}` : ''}
                    </span>
                  </button>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section className="upload" id="import">
          <div className="panel">
            <div className="panel__header">
              <h3>Import Custom JSON</h3>
              <span>Paste JSON text, upload a file, or create a new custom table</span>
            </div>
            <div className="form">
              <div className="form__group">
                <label className="form__label">JSON Text</label>
                <textarea
                  className="form__textarea"
                  value={importText}
                  onChange={(event) => setImportText(event.target.value)}
                  placeholder='{"repository": {...}, "competitions": [...]} or [{"team":"Silver Strikers","points":30}]'
                  rows={10}
                />
              </div>
              <div className="upload__body">
                <input
                  className="upload__input"
                  type="file"
                  accept="application/json"
                  onChange={(event) => setImportFile(event.target.files?.[0] ?? null)}
                />
                <span className="upload__status">Text input overrides the file when both are provided.</span>
              </div>
              <div className="upload__body">
                <Button onClick={handleImport}>Import Repository JSON</Button>
              </div>
              <div className="form__group">
                <label className="form__label">Custom Table Name</label>
                <input
                  className="form__input"
                  type="text"
                  value={dynamicTableName}
                  onChange={(event) => setDynamicTableName(event.target.value)}
                  placeholder="e.g. malawi_sports_weekly_form"
                />
              </div>
              <div className="upload__body">
                <Button variant="ghost" onClick={handleDynamicIngest}>Create / Append Data Table</Button>
              </div>
            </div>
            {importStatus === 'loading' ? <p className="upload__status">Importing repository data...</p> : null}
            {importStatus === 'success' ? (
              <p className="upload__status upload__status--success">Repository import complete.</p>
            ) : null}
            {importStatus === 'error' ? (
              <p className="upload__status upload__status--error">{importError}</p>
            ) : null}
            {dynamicStatus === 'loading' ? <p className="upload__status">Creating or updating custom table...</p> : null}
            {dynamicStatus === 'success' ? (
              <p className="upload__status upload__status--success">Custom table updated successfully.</p>
            ) : null}
            {dynamicStatus === 'error' ? (
              <p className="upload__status upload__status--error">{dynamicError}</p>
            ) : null}
            {dynamicPreview.length > 0 ? (
              <pre className="json-preview">{JSON.stringify(dynamicPreview, null, 2)}</pre>
            ) : null}
          </div>
        </section>

        <section className="upload" id="insights">
          <div className="panel">
            <div className="panel__header">
              <h3>OpenRouter AI Insights</h3>
              <span>Generate sweeter Malawian sports analysis from imported data</span>
            </div>
            <div className="form">
              <div className="form__group">
                <label className="form__label">Prompt</label>
                <textarea
                  className="form__textarea"
                  value={analysisPrompt}
                  onChange={(event) => setAnalysisPrompt(event.target.value)}
                  rows={5}
                />
              </div>
              <div className="form__group">
                <label className="form__label">Custom Data Table</label>
                <select
                  className="form__select"
                  value={analysisTable}
                  onChange={(event) => setAnalysisTable(event.target.value)}
                >
                  <option value="">No custom table selected</option>
                  {dynamicTables.map((table) => (
                    <option key={table.table_name} value={table.table_name}>
                      {table.table_name} ({table.row_count} rows)
                    </option>
                  ))}
                </select>
              </div>
              <div className="form__group">
                <label className="form__label">Extra JSON Context</label>
                <textarea
                  className="form__textarea"
                  value={analysisContext}
                  onChange={(event) => setAnalysisContext(event.target.value)}
                  placeholder='{"focus":"TNM Super League strikers","notes":["compare goals with points"]}'
                  rows={6}
                />
              </div>
              <div className="upload__body">
                <Button onClick={handleAnalyze}>Run AI Analysis</Button>
              </div>
            </div>
            {analysisStatus === 'loading' ? <p className="upload__status">Generating analysis...</p> : null}
            {analysisStatus === 'error' ? (
              <p className="upload__status upload__status--error">{analysisError}</p>
            ) : null}
            {analysisResponse ? <div className="ai-panel">{analysisResponse}</div> : null}
          </div>
        </section>
      </main>

      {showLogin && (
        <div className="modal-overlay">
          <div className="panel modal-content">
            <div className="panel__header">
              <h3>Scout & Agent Login</h3>
              <button onClick={() => setShowLogin(false)}>&times;</button>
            </div>
            <form onSubmit={handleLogin} className="form">
              <div className="form__group">
                <label className="form__label">Username</label>
                <input className="form__input" value={loginForm.username} onChange={e => setLoginForm({...loginForm, username: e.target.value})} required />
              </div>
              <div className="form__group">
                <label className="form__label">Password</label>
                <input className="form__input" type="password" value={loginForm.password} onChange={e => setLoginForm({...loginForm, password: e.target.value})} required />
              </div>
              <Button type="submit" style={{ width: '100%' }}>Login</Button>
              <p style={{ marginTop: '12px', fontSize: '11px', color: 'var(--muted)' }}>
                Access restricted to verified scouts and agents.
              </p>
            </form>
          </div>
        </div>
      )}

      {transferCard && (
        <div className="modal-overlay">
          <div className="panel modal-content" style={{ maxWidth: '800px', width: '90%' }}>
            <div className="panel__header">
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <TeamMark text={transferCard.player.team_name.substring(0,2).toUpperCase()} />
                <div>
                  <h3 style={{ margin: 0 }}>{transferCard.player.name}</h3>
                  <small style={{ color: 'var(--blue)' }}>{transferCard.player.position} | {transferCard.player.team_name}</small>
                </div>
              </div>
              <button onClick={() => setTransferCard(null)}>&times;</button>
            </div>
            
            <div className="transfer-card-grid">
              <div className="card-section">
                <h4>Personal Details</h4>
                <div className="details-list">
                  <div><span>Citizenship</span> <strong>{transferCard.player.citizenship || 'N/A'}</strong></div>
                  <div><span>Preferred Foot</span> <strong>{transferCard.player.preferred_foot || 'N/A'}</strong></div>
                  <div><span>Height</span> <strong>{transferCard.player.height_cm ? `${transferCard.player.height_cm}cm` : 'N/A'}</strong></div>
                  <div><span>Weight</span> <strong>{transferCard.player.weight_kg ? `${transferCard.player.weight_kg}kg` : 'N/A'}</strong></div>
                  <div><span>Age</span> <strong>{transferCard.player.date_of_birth ? new Date().getFullYear() - new Date(transferCard.player.date_of_birth).getFullYear() : 'N/A'}</strong></div>
                </div>
              </div>

              <div className="card-section">
                <h4>Contract & Agent</h4>
                <div className="details-list">
                  <div><span>Expires</span> <strong style={{ color: 'var(--blue)' }}>{transferCard.player.contract_expires || 'N/A'}</strong></div>
                  <div><span>Status</span> <strong>{transferCard.player.is_transfer_listed ? <Badge>Transfer Listed</Badge> : 'Active'}</strong></div>
                  <div><span>Agency</span> <strong>{transferCard.player.agent_name || 'Independent'}</strong></div>
                </div>
              </div>

              <div className="card-section" style={{ gridColumn: 'span 2' }}>
                <h4>Market Value Trend</h4>
                <div style={{ height: '60px', background: 'var(--surface-light)', borderRadius: '4px', display: 'flex', alignItems: 'flex-end', padding: '8px', gap: '4px' }}>
                  {transferCard.valuation_history.map((v, i) => (
                    <div key={i} style={{ flex: 1, background: 'var(--blue)', height: `${(v.valuation / transferCard.player.market_value) * 100}%`, minHeight: '4px' }} title={v.date}></div>
                  ))}
                  {transferCard.valuation_history.length === 0 && <span style={{ fontSize: '11px', color: 'var(--muted)' }}>No historical data available.</span>}
                </div>
                <div style={{ marginTop: '8px', textAlign: 'right' }}>
                  <small>Current Valuation:</small> 
                  <strong style={{ display: 'block', fontSize: '18px', color: 'var(--blue)' }}>{transferCard.player.currency} {new Intl.NumberFormat().format(transferCard.player.market_value)}</strong>
                </div>
              </div>

              <div className="card-section">
                <h4>Recent Highlights</h4>
                <ul style={{ paddingLeft: '20px', fontSize: '12px' }}>
                  {transferCard.highlights.map((h, i) => <li key={i}>{h}</li>)}
                  {transferCard.highlights.length === 0 && <li>No recent match highlights.</li>}
                </ul>
              </div>

              <div className="card-section">
                <h4>Scout Actions</h4>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                  <Button style={{ width: '100%' }}>Download Transfer Card (PDF)</Button>
                  <Button variant="ghost" style={{ width: '100%' }}>Contact Agent</Button>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
