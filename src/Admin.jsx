import { useState, useEffect } from 'react'

export default function Admin({ onRefresh }) {
  const [activeTab, setActiveTab] = useState('manual')
  const [activeSubTab, setActiveSubTab] = useState('competition')
  
  const [competitions, setCompetitions] = useState([])
  const [teams, setTeams] = useState([])
  
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

  useEffect(() => {
    fetchCompetitions()
    fetchTeams()
    fetchDynamicTables()
  }, [])

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
  const [imageExtractPrompt, setImageExtractPrompt] = useState('Extract the sports data in this image into clean JSON. Use arrays and objects where appropriate, and preserve team names, scores, standings, player stats, dates, and notes when visible.')
  const [extractedJson, setExtractedJson] = useState('')
  const [extractedRaw, setExtractedRaw] = useState('')
  const [pictureTableName, setPictureTableName] = useState('malawi_sports_image_extract')
  const [pictureSaveMode, setPictureSaveMode] = useState('dynamic')
  const [picturePreviewUrl, setPicturePreviewUrl] = useState('')

  function syncPicturePreview(file) {
    if (!file) {
      setPicturePreviewUrl('')
      return
    }
    setPicturePreviewUrl(URL.createObjectURL(file))
  }

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

  async function handleExtractImageJson() {
    if (!uploadFile) {
      setError('Choose an image first.')
      return
    }
    setStatus('Extracting JSON from image...')
    setError('')
    setExtractedJson('')
    setExtractedRaw('')
    const formData = new FormData()
    formData.append('file', uploadFile)
    formData.append('prompt', imageExtractPrompt)
    try {
      const res = await fetch(`${apiBaseUrl}/api/ai/extract-json-from-image`, {
        method: 'POST',
        body: formData,
      })
      const result = await res.json()
      if (!res.ok) {
        throw new Error(result.detail || 'Image extraction failed')
      }
      setExtractedJson(JSON.stringify(result.json_data, null, 2))
      setExtractedRaw(result.raw_response || '')
      setStatus(`Image extracted with ${result.model}`)
    } catch (e) {
      setError(e.message)
    }
  }

  async function handleSaveExtractedJson() {
    setStatus('Saving extracted JSON...')
    setError('')
    try {
      if (!extractedJson.trim()) {
        throw new Error('There is no extracted JSON to save.')
      }
      const parsed = JSON.parse(extractedJson)
      if (pictureSaveMode === 'repository') {
        const res = await fetch(`${apiBaseUrl}/api/repository/import`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(parsed),
        })
        const result = await res.json()
        if (!res.ok || result.error) throw new Error(result.error || 'Repository import failed')
        setStatus('Extracted JSON imported into the main repository data.')
        onRefresh()
        return
      }

      if (!pictureTableName.trim()) {
        throw new Error('Custom table name is required.')
      }
      const data = Array.isArray(parsed) ? parsed : [parsed]
      const res = await fetch(`${apiBaseUrl}/api/admin/ingest-dynamic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: pictureTableName,
          create_if_missing: true,
          data,
        }),
      })
      const result = await res.json()
      if (!res.ok || result.error) throw new Error(result.error || 'Dynamic ingestion failed')
      setStatus(result.message)
      setDynamicPreview(result.preview || [])
      setAiTableName(result.table_name || pictureTableName)
      fetchDynamicTables()
    } catch (e) {
      setError(e.message)
    }
  }

  return (
    <section className="panel" id="admin">
      <div className="panel__header">
        <h3>Data Management</h3>
        <span>Collect competitions, teams, matches, images, JSON, and AI-assisted extracts</span>
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
            <button className={`tab ${activeSubTab === 'dynamic' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('dynamic')}>Dynamic Ingest</button>
            <button className={`tab ${activeSubTab === 'ai' ? 'tab--active' : ''}`} onClick={() => setActiveSubTab('ai')}>AI Analyst</button>
          </div>

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
          <div className="form">
            <div className="upload__body" style={{ marginBottom: '8px' }}>
              <input
                className="upload__input"
                type="file"
                onChange={e => {
                  const file = e.target.files?.[0] || null
                  setUploadFile(file)
                  syncPicturePreview(file)
                }}
                accept="image/*"
              />
              <button className="btn btn--primary" onClick={handleUpload}>Upload Image</button>
              <button className="btn btn--ghost" onClick={handleExtractImageJson}>Extract JSON</button>
            </div>

            <div className="form__group">
              <label className="form__label">Extraction Prompt</label>
              <textarea
                className="form__textarea"
                value={imageExtractPrompt}
                onChange={e => setImageExtractPrompt(e.target.value)}
                rows={4}
              />
            </div>

            {picturePreviewUrl && (
              <div className="image-review">
                <img src={picturePreviewUrl} alt="Selected upload" className="image-review__preview" />
              </div>
            )}

            <div className="form__row">
              <div className="form__group">
                <label className="form__label">Save Mode</label>
                <select className="form__select" value={pictureSaveMode} onChange={e => setPictureSaveMode(e.target.value)}>
                  <option value="dynamic">Custom Table</option>
                  <option value="repository">Repository Import</option>
                </select>
              </div>
              <div className="form__group">
                <label className="form__label">Custom Table Name</label>
                <input
                  className="form__input"
                  value={pictureTableName}
                  onChange={e => setPictureTableName(e.target.value)}
                  disabled={pictureSaveMode !== 'dynamic'}
                  placeholder="e.g. malawi_fixture_card_extract"
                />
              </div>
            </div>

            <div className="form__group">
              <label className="form__label">Review And Edit Extracted JSON</label>
              <textarea
                className="form__textarea"
                style={{ minHeight: '260px', fontFamily: 'monospace' }}
                value={extractedJson}
                onChange={e => setExtractedJson(e.target.value)}
                placeholder='Extracted JSON will appear here after you run image extraction.'
              />
            </div>

            <div className="upload__body">
              <button className="btn btn--primary" onClick={handleSaveExtractedJson}>Confirm And Save To Database</button>
            </div>

            {extractedRaw && (
              <div className="ai-panel">
                <strong>Raw AI Response:</strong><br />
                {extractedRaw}
              </div>
            )}
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
