import { useState, useEffect } from 'react'

export default function Admin({ onRefresh }) {
  const [activeTab, setActiveTab] = useState('manual')
  const [activeSubTab, setActiveSubTab] = useState('competition')
  
  const [competitions, setCompetitions] = useState([])
  const [teams, setTeams] = useState([])
  const [players, setPlayers] = useState([])
  const [agents, setAgents] = useState([])
  
  const [status, setStatus] = useState('')
  const [error, setError] = useState('')
  const [dynamicTables, setDynamicTables] = useState([])
  const [dynamicPreview, setDynamicPreview] = useState([])

  const [dynamicTableName, setDynamicTableName] = useState('')
  const [dynamicJson, setDynamicJson] = useState('')
  const [aiPrompt, setAiPrompt] = useState('Provide a detailed analysis of the recent performance of top Malawian strikers and suggest who could be the next breakout star.')
  const [aiTableName, setAiTableName] = useState('')
  const [aiDataContext, setAiDataContext] = useState('')
  const [aiResponse, setAiResponse] = useState('')

  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://127.0.0.1:8000'
  const token = localStorage.getItem('token')

  useEffect(() => {
    fetchCompetitions()
    fetchTeams()
    fetchValuations()
    fetchDynamicTables()
    if (token) fetchAgents()
  }, [])

  async function fetchAgents() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/agents`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      if (res.ok) setAgents(await res.json())
    } catch (e) { console.error(e) }
  }

  const [agentForm, setAgentForm] = useState({ name: '', agency_name: '', fifa_license_number: '', whatsapp_number: '', email: '' })
  async function handleCreateAgent(e) {
    e.preventDefault()
    setStatus('Saving...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/agents`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify(agentForm)
      })
      if (!res.ok) throw new Error('Failed to save agent')
      setStatus('Agent saved!')
      fetchAgents()
    } catch (e) { setError(e.message) }
  }

  const [playerForm, setPlayerForm] = useState({
    name: '', team_id: '', position: '', height_cm: '', weight_kg: '', 
    preferred_foot: 'Right', date_of_birth: '', citizenship: 'Malawi', 
    contract_expires: '', agent_id: ''
  })
  async function handleCreatePlayer(e) {
    e.preventDefault()
    setStatus('Saving...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/players`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          ...playerForm,
          team_id: parseInt(playerForm.team_id),
          agent_id: playerForm.agent_id ? parseInt(playerForm.agent_id) : null,
          height_cm: playerForm.height_cm ? parseInt(playerForm.height_cm) : null,
          weight_kg: playerForm.weight_kg ? parseInt(playerForm.weight_kg) : null,
        })
      })
      if (!res.ok) throw new Error('Failed to save player profile')
      setStatus('Player profile saved!')
      fetchValuations()
    } catch (e) { setError(e.message) }
  }

  async function fetchCompetitions() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/competitions`)
      if (res.ok) setCompetitions(await res.json())
    } catch (e) { console.error(e) }
  }

  async function fetchTeams() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/teams`)
      if (res.ok) setTeams(await res.json())
    } catch (e) { console.error(e) }
  }

  async function fetchValuations() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/market-valuations`)
      if (res.ok) setPlayers(await res.json())
    } catch (e) { console.error(e) }
  }

  async function fetchDynamicTables() {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/dynamic-tables`)
      if (!res.ok) return
      const data = await res.json()
      setDynamicTables(data)
      setAiTableName(current => current || data[0]?.table_name || '')
    } catch (e) { console.error(e) }
  }

  async function fetchDynamicPreview(tableName) {
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/dynamic-tables/${tableName}?limit=5`)
      if (!res.ok) return
      const data = await res.json()
      setDynamicPreview(data.rows || [])
    } catch (e) { console.error(e) }
  }

  const [compForm, setCompForm] = useState({ name: '', season_year: 2025, country: '', current_season: '2025' })
  async function handleCreateCompetition(e) {
    e.preventDefault()
    setStatus('Saving...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/competitions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(compForm)
      })
      if (!res.ok) throw new Error('Failed to save competition')
      setStatus('Competition saved!')
      fetchCompetitions()
      onRefresh()
    } catch (e) { setError(e.message) }
  }

  const [teamForm, setTeamForm] = useState({ name: '', short_name: '', city: '', icon_text: '' })
  async function handleCreateTeam(e) {
    e.preventDefault()
    setStatus('Saving...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/teams`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(teamForm)
      })
      if (!res.ok) throw new Error('Failed to save team')
      setStatus('Team saved!')
      fetchTeams()
      onRefresh()
    } catch (e) { setError(e.message) }
  }

  const [matchForm, setMatchForm] = useState({
    competition_id: '', week_label: 'Week 1', home_team: '', away_team: '',
    home_score: 0, away_score: 0, source_image: 'manual-entry', image_url: ''
  })
  async function handleCreateMatch(e) {
    e.preventDefault()
    setStatus('Saving...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/matches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...matchForm, competition_id: parseInt(matchForm.competition_id) })
      })
      if (!res.ok) throw new Error('Failed to save match')
      setStatus('Match saved!')
      onRefresh()
    } catch (e) { setError(e.message) }
  }

  const [valForm, setValForm] = useState({ player_id: '', market_value: 0, currency: 'MWK', is_premium: false })
  async function handleUpdateValuation(e) {
    e.preventDefault()
    setStatus('Updating...')
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/market-valuations`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...valForm, player_id: parseInt(valForm.player_id), market_value: parseFloat(valForm.market_value) })
      })
      if (!res.ok) throw new Error('Failed to update valuation')
      setStatus('Valuation updated!')
      fetchValuations()
      onRefresh()
    } catch (e) { setError(e.message) }
  }

  async function handleDynamicIngest() {
    setStatus('Ingesting...')
    setError('')
    setDynamicPreview([])
    try {
      if (!dynamicTableName.trim()) throw new Error('Table name is required')
      const parsed = JSON.parse(dynamicJson)
      const data = Array.isArray(parsed) ? parsed : [parsed]
      const res = await fetch(`${apiBaseUrl}/api/admin/ingest-dynamic`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ table_name: dynamicTableName, create_if_missing: true, data })
      })
      const result = await res.json()
      if (!res.ok || result.error) throw new Error(result.error || 'Ingestion failed')
      setStatus(result.message)
      setAiTableName(result.table_name || dynamicTableName)
      setDynamicPreview(result.preview || [])
      fetchDynamicTables()
    } catch (e) { setError(e.message) }
  }

  async function handleAIAnalyze() {
    setStatus('Analyzing...')
    setError('')
    setAiResponse('')
    try {
      let dataContext = null
      if (aiDataContext.trim()) {
        dataContext = JSON.parse(aiDataContext)
      }
      const res = await fetch(`${apiBaseUrl}/api/ai/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: aiPrompt, table_name: aiTableName || null, row_limit: 20, data_context: dataContext })
      })
      const result = await res.json()
      if (!res.ok || result.error) throw new Error(result.error || 'Analysis failed')
      setAiResponse(result.analysis)
      setStatus('Analysis complete!')
    } catch (e) { setError(e.message) }
  }

  const [uploadFile, setUploadFile] = useState(null)
  const [uploadedImages, setUploadedImages] = useState([])
  async function handleUpload(e) {
    e.preventDefault()
    if (!uploadFile) return
    setStatus('Uploading...')
    const formData = new FormData()
    formData.append('file', uploadFile)
    try {
      const res = await fetch(`${apiBaseUrl}/api/admin/upload-image`, {
        method: 'POST',
        body: formData
      })
      if (!res.ok) throw new Error('Upload failed')
      const data = await res.json()
      setUploadedImages([data, ...uploadedImages])
      setStatus('Upload successful!')
      if (activeSubTab === 'match') {
        setMatchForm({ ...matchForm, image_url: data.url })
      }
    } catch (e) { setError(e.message) }
  }

  return (
    <section className="panel" id="admin">
      <div className="panel__header">
        <h3>Data Management</h3>
        <span>Add records, manage market values and premium data</span>
      </div>

      <div className="tabs">
        <button className={`tab ${activeTab === 'manual' ? 'tab--active' : ''}`} onClick={() => setActiveTab('manual')}>Manual Entry</button>
        <button className={`tab ${activeTab === 'pictures' ? 'tab--active' : ''}`} onClick={() => setActiveTab('pictures')}>Pictures / Uploads</button>
      </div>

      {activeTab === 'manual' && (
        <div className="admin__manual">
          <div className="tabs" style={{ fontSize: '11px' }}>
            <button className={`tab ${activeSubTab === 'competition' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('competition')}>Competition</button>
            <button className={`tab ${activeSubTab === 'team' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('team')}>Team</button>
            <button className={`tab ${activeSubTab === 'match' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('match')}>Match</button>
            <button className={`tab ${activeSubTab === 'player' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('player')}>Player Profile</button>
            <button className={`tab ${activeSubTab === 'agent' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('agent')}>Agent</button>
            <button className={`tab ${activeSubTab === 'valuation' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('valuation')}>Valuations</button>
            <button className={`tab ${activeSubTab === 'dynamic' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('dynamic')}>Dynamic Ingest</button>
            <button className={`tab ${activeSubTab === 'ai' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('ai')}>AI Analyst</button>
          </div>

          {activeSubTab === 'agent' && (
            <form className="form" onSubmit={handleCreateAgent}>
              <div className="form__group">
                <label className="form__label">Agent Name</label>
                <input className="form__input" value={agentForm.name} onChange={e => setAgentForm({...agentForm, name: e.target.value})} required />
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Agency</label>
                  <input className="form__input" value={agentForm.agency_name} onChange={e => setAgentForm({...agentForm, agency_name: e.target.value})} />
                </div>
                <div className="form__group">
                  <label className="form__label">License #</label>
                  <input className="form__input" value={agentForm.fifa_license_number} onChange={e => setAgentForm({...agentForm, fifa_license_number: e.target.value})} />
                </div>
              </div>
              <div className="form__group">
                <label className="form__label">WhatsApp</label>
                <input className="form__input" value={agentForm.whatsapp_number} onChange={e => setAgentForm({...agentForm, whatsapp_number: e.target.value})} />
              </div>
              <button className="btn btn--primary" type="submit">Save Agent</button>
            </form>
          )}

          {activeSubTab === 'player' && (
            <form className="form" onSubmit={handleCreatePlayer}>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Name</label>
                  <input className="form__input" value={playerForm.name} onChange={e => setPlayerForm({...playerForm, name: e.target.value})} required />
                </div>
                <div className="form__group">
                  <label className="form__label">Team</label>
                  <select className="form__select" value={playerForm.team_id} onChange={e => setPlayerForm({...playerForm, team_id: e.target.value})} required>
                    <option value="">Select Team</option>
                    {teams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                  </select>
                </div>
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Position</label>
                  <input className="form__input" value={playerForm.position} onChange={e => setPlayerForm({...playerForm, position: e.target.value})} placeholder="e.g. Center Forward" />
                </div>
                <div className="form__group">
                  <label className="form__label">Preferred Foot</label>
                  <select className="form__select" value={playerForm.preferred_foot} onChange={e => setPlayerForm({...playerForm, preferred_foot: e.target.value})}>
                    <option value="Right">Right</option>
                    <option value="Left">Left</option>
                    <option value="Both">Both</option>
                  </select>
                </div>
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Height (cm)</label>
                  <input className="form__input" type="number" value={playerForm.height_cm} onChange={e => setPlayerForm({...playerForm, height_cm: e.target.value})} />
                </div>
                <div className="form__group">
                  <label className="form__label">Agent</label>
                  <select className="form__select" value={playerForm.agent_id} onChange={e => setPlayerForm({...playerForm, agent_id: e.target.value})}>
                    <option value="">No Agent</option>
                    {agents.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
                  </select>
                </div>
              </div>
              <button className="btn btn--primary" type="submit">Save Player Profile</button>
            </form>
          )}

          {activeSubTab === 'competition' && (
            <form className="form" onSubmit={handleCreateCompetition}>
              <div className="form__group">
                <label className="form__label">Name</label>
                <input className="form__input" value={compForm.name} onChange={e => setCompForm({...compForm, name: e.target.value})} required />
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Year</label>
                  <input className="form__input" type="number" value={compForm.season_year} onChange={e => setCompForm({...compForm, season_year: parseInt(e.target.value)})} required />
                </div>
                <div className="form__group">
                  <label className="form__label">Country</label>
                  <input className="form__input" value={compForm.country} onChange={e => setCompForm({...compForm, country: e.target.value})} />
                </div>
              </div>
              <button className="btn btn--primary" type="submit">Create Competition</button>
            </form>
          )}

          {activeSubTab === 'team' && (
            <form className="form" onSubmit={handleCreateTeam}>
              <div className="form__group">
                <label className="form__label">Team Name</label>
                <input className="form__input" value={teamForm.name} onChange={e => setTeamForm({...teamForm, name: e.target.value})} required />
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Short Name</label>
                  <input className="form__input" value={teamForm.short_name} onChange={e => setTeamForm({...teamForm, short_name: e.target.value})} required />
                </div>
                <div className="form__group">
                  <label className="form__label">City</label>
                  <input className="form__input" value={teamForm.city} onChange={e => setTeamForm({...teamForm, city: e.target.value})} required />
                </div>
              </div>
              <div className="form__group">
                <label className="form__label">Icon Text</label>
                <input className="form__input" value={teamForm.icon_text} onChange={e => setTeamForm({...teamForm, icon_text: e.target.value})} required maxLength={3} />
              </div>
              <button className="btn btn--primary" type="submit">Create Team</button>
            </form>
          )}

          {activeSubTab === 'match' && (
            <form className="form" onSubmit={handleCreateMatch}>
              <div className="form__group">
                <label className="form__label">Competition</label>
                <select className="form__select" value={matchForm.competition_id} onChange={e => setMatchForm({...matchForm, competition_id: e.target.value})} required>
                  <option value="">Select Competition</option>
                  {competitions.map(c => <option key={c.id} value={c.id}>{c.name} {c.season_year}</option>)}
                </select>
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Home Team</label>
                  <select className="form__select" value={matchForm.home_team} onChange={e => setMatchForm({...matchForm, home_team: e.target.value})} required>
                    <option value="">Select Home</option>
                    {teams.map(t => <option key={t.id} value={t.name}>{t.name}</option>)}
                  </select>
                </div>
                <div className="form__group">
                  <label className="form__label">Away Team</label>
                  <select className="form__select" value={matchForm.away_team} onChange={e => setMatchForm({...matchForm, away_team: e.target.value})} required>
                    <option value="">Select Away</option>
                    {teams.map(t => <option key={t.id} value={t.name}>{t.name}</option>)}
                  </select>
                </div>
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Home Score</label>
                  <input className="form__input" type="number" value={matchForm.home_score} onChange={e => setMatchForm({...matchForm, home_score: parseInt(e.target.value)})} required />
                </div>
                <div className="form__group">
                  <label className="form__label">Away Score</label>
                  <input className="form__input" type="number" value={matchForm.away_score} onChange={e => setMatchForm({...matchForm, away_score: parseInt(e.target.value)})} required />
                </div>
              </div>
              <div className="form__group">
                <label className="form__label">Image URL</label>
                <input className="form__input" value={matchForm.image_url} onChange={e => setMatchForm({...matchForm, image_url: e.target.value})} />
              </div>
              <button className="btn btn--primary" type="submit">Create Match</button>
            </form>
          )}

          {activeSubTab === 'valuation' && (
            <form className="form" onSubmit={handleUpdateValuation}>
              <div className="form__group">
                <label className="form__label">Player</label>
                <select className="form__select" value={valForm.player_id} onChange={e => setValForm({...valForm, player_id: e.target.value})} required>
                  <option value="">Select Player</option>
                  {players.map(p => <option key={p.player_id} value={p.player_id}>{p.player_name} ({p.team_name})</option>)}
                </select>
              </div>
              <div className="form__row">
                <div className="form__group">
                  <label className="form__label">Market Value</label>
                  <input className="form__input" type="number" value={valForm.market_value} onChange={e => setValForm({...valForm, market_value: e.target.value})} required />
                </div>
                <div className="form__group">
                  <label className="form__label">Currency</label>
                  <input className="form__input" value={valForm.currency} onChange={e => setValForm({...valForm, currency: e.target.value})} required />
                </div>
              </div>
              <div className="form__group">
                <label className="form__label" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <input type="checkbox" checked={valForm.is_premium} onChange={e => setValForm({...valForm, is_premium: e.target.checked})} />
                  Mark as Premium Data
                </label>
              </div>
              <button className="btn btn--primary" type="submit">Update Market Value</button>
            </form>
          )}

          {activeSubTab === 'dynamic' && (
            <div className="form">
              <div className="form__group">
                <label className="form__label">Table Name</label>
                <input className="form__input" value={dynamicTableName} onChange={e => setDynamicTableName(e.target.value)} placeholder="e.g. malawi_league_extra" />
              </div>
              <div className="form__group">
                <label className="form__label">JSON Data</label>
                <textarea 
                  className="form__textarea" 
                  style={{ minHeight: '200px', fontFamily: 'monospace' }} 
                  value={dynamicJson} 
                  onChange={e => setDynamicJson(e.target.value)}
                  placeholder='[{"player": "Gabadinho", "goals": 15}, ...] or {"club":"Silver Strikers","points":30}'
                />
              </div>
              <button className="btn btn--primary" onClick={handleDynamicIngest}>Ingest Data</button>
              {dynamicTables.length > 0 && (
                <div className="form__group">
                  <label className="form__label">Existing Custom Tables</label>
                  <select
                    className="form__select"
                    value={aiTableName}
                    onChange={e => {
                      setAiTableName(e.target.value)
                      if (e.target.value) fetchDynamicPreview(e.target.value)
                    }}
                  >
                    <option value="">Select custom table</option>
                    {dynamicTables.map(table => (
                      <option key={table.table_name} value={table.table_name}>
                        {table.table_name} ({table.row_count} rows)
                      </option>
                    ))}
                  </select>
                </div>
              )}
              {dynamicPreview.length > 0 && (
                <pre className="json-preview">{JSON.stringify(dynamicPreview, null, 2)}</pre>
              )}
            </div>
          )}

          {activeSubTab === 'ai' && (
            <div className="form">
              <div className="form__group">
                <label className="form__label">AI Prompt</label>
                <textarea 
                  className="form__textarea" 
                  style={{ minHeight: '100px' }} 
                  value={aiPrompt} 
                  onChange={e => setAiPrompt(e.target.value)}
                />
              </div>
              <div className="form__group">
                <label className="form__label">Use Custom Table</label>
                <select className="form__select" value={aiTableName} onChange={e => setAiTableName(e.target.value)}>
                  <option value="">No custom table selected</option>
                  {dynamicTables.map(table => (
                    <option key={table.table_name} value={table.table_name}>
                      {table.table_name} ({table.row_count} rows)
                    </option>
                  ))}
                </select>
              </div>
              <div className="form__group">
                <label className="form__label">Data Context (Optional JSON)</label>
                <textarea 
                  className="form__textarea" 
                  style={{ minHeight: '100px', fontFamily: 'monospace' }} 
                  value={aiDataContext} 
                  onChange={e => setAiDataContext(e.target.value)}
                  placeholder='{"players": [...]}'
                />
              </div>
              <button className="btn btn--primary" onClick={handleAIAnalyze}>Ask AI Analyst</button>
              
              {aiResponse && (
                <div className="ai-panel">
                  <strong>AI Insight:</strong><br/>
                  {aiResponse}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {activeTab === 'pictures' && (
        <div className="admin__pictures">
          <div className="upload__body" style={{ marginBottom: '20px' }}>
            <input className="upload__input" type="file" onChange={e => setUploadFile(e.target.files[0])} accept="image/*" />
            <button className="btn btn--primary" onClick={handleUpload}>Upload Image</button>
          </div>
          
          <div className="gallery">
            {uploadedImages.map((img, i) => (
              <div key={i} className="gallery__item" onClick={() => {
                setMatchForm({...matchForm, image_url: img.url})
                setActiveTab('manual')
                setActiveSubTab('match')
              }}>
                <img src={img.url} className="gallery__img" alt={img.filename} />
                <span className="gallery__name">{img.filename}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {status && <p className="upload__status upload__status--success" style={{ marginTop: '12px' }}>{status}</p>}
      {error && <p className="upload__status upload__status--error" style={{ marginTop: '12px' }}>{error}</p>}
    </section>
  )
}
