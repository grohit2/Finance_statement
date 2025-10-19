import React, { useEffect, useMemo, useState } from 'react'
import { loadShareIndex, loadTransactions } from './hooks/useTransactions.js'
import ProfileSelector from './components/ProfileSelector.jsx'
import DateRangePicker from './components/DateRangePicker.jsx'
import DailyBarChart from './components/DailyBarChart.jsx'
import CategoryPieChart from './components/CategoryPieChart.jsx'
import CategoryBarChart from './components/CategoryBarChart.jsx'
import Drilldown from './components/Drilldown.jsx'
import * as d3 from 'd3'

export default function App() {
  const [shares, setShares] = useState([])
  const [viewer, setViewer] = useState('')
  const [owner, setOwner] = useState('')
  const [tx, setTx] = useState([])

  const [from, setFrom] = useState(null)
  const [to, setTo] = useState(null)
  const [catView, setCatView] = useState('pie') // 'pie' | 'bar'
  const [drill, setDrill] = useState(null)      // { type: 'category'|'day', value: ... }

  useEffect(() => {
    loadShareIndex().then(rows => {
      setShares(rows)
      const viewers = [...new Set(rows.map(r => r.viewer_profile_id))]
      const params = new URLSearchParams(window.location.search)
      const v0 = params.get('viewer') || viewers[0]
      setViewer(v0)
      const ownerOptions = rows.filter(r => r.viewer_profile_id===v0).map(r => r.owner_profile_id)
      setOwner(ownerOptions[0])
    })
  }, [])

  useEffect(() => { if (owner) loadTransactions(owner).then(setTx) }, [owner])

  // Compute data bounds and default month (most recent month in data)
  const dataMin = useMemo(() => tx.length ? d3.min(tx, d => d.date) : null, [tx])
  const dataMax = useMemo(() => tx.length ? d3.max(tx, d => d.date) : null, [tx])

  useEffect(() => {
    if (!tx.length) return
    const max = d3.max(tx, d => d.date)
    const start = new Date(max.getFullYear(), max.getMonth(), 1)
    const end = new Date(max.getFullYear(), max.getMonth()+1, 0)
    setFrom(start); setTo(end)
  }, [tx])

  // Filter by date range
  const filtered = useMemo(() => {
    if (!from || !to) return []
    const a = new Date(from.getFullYear(), from.getMonth(), from.getDate())
    const b = new Date(to.getFullYear(), to.getMonth(), to.getDate(), 23,59,59,999)
    return tx.filter(r => r.date >= a && r.date <= b)
  }, [tx, from, to])

  function onRangeChange({from: f, to: t}) {
    setFrom(f); setTo(t); setDrill(null) // reset drill on range change
  }

  return (
    <div className="container">
      <h2>Personal Finance Dashboard — Monthly Spending</h2>

      <div className="card">
        <ProfileSelector
          viewer={viewer}
          owners={shares}
          owner={owner}
          onChangeViewer={(v) => {
            setViewer(v)
            const ownerOptions = shares.filter(o => o.viewer_profile_id===v).map(o => o.owner_profile_id)
            setOwner(ownerOptions[0] || '')
          }}
          onChangeOwner={(o) => { setOwner(o); setDrill(null) }}
        />
        <DateRangePicker
          minDate={dataMin}
          maxDate={dataMax}
          from={from}
          to={to}
          onChange={onRangeChange}
        />
      </div>

      <div style={{marginTop:16}} className="card">
        <h3 style={{margin:'0 0 8px'}}>Daily Spending</h3>
        <DailyBarChart
          data={filtered}
          onBarClick={(day) => setDrill({ type:'day', value: day })}
        />
      </div>

      <div style={{marginTop:16}} className="card">
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'center'}}>
          <h3 style={{margin:0}}>Spending by Category</h3>
          <div className="toggle">
            <label><input type="radio" name="catview" checked={catView==='pie'} onChange={()=>setCatView('pie')} /> Pie</label>
            <label><input type="radio" name="catview" checked={catView==='bar'} onChange={()=>setCatView('bar')} /> Bar</label>
          </div>
        </div>
        {catView === 'pie'
          ? <CategoryPieChart data={filtered} onSliceClick={(c)=>setDrill({type:'category', value:c})} />
          : <CategoryBarChart data={filtered} onBarClick={(c)=>setDrill({type:'category', value:c})} />
        }
      </div>

      {drill && (
        <Drilldown
          data={filtered}
          type={drill.type}
          value={drill.value}
          from={from}
          to={to}
          onClose={() => setDrill(null)}
        />
      )}

      <p style={{color:'#666', marginTop:16}}>
        Data source: <code>/public/data/…</code> (symlink to <code>data/exports/csv</code>). Owner: <b>{owner}</b>, Viewer: <b>{viewer}</b>.
      </p>
    </div>
  )
}
