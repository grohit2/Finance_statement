import React, { useMemo, useState } from 'react'

function toInputDate(d) { return d ? new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0,10) : '' }
function fromInputDate(s) { return s ? new Date(`${s}T00:00:00`) : null }

function startOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(), 1) }
function endOfMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1, 0) }
function startOfQuarter(d){ return new Date(d.getFullYear(), Math.floor(d.getMonth()/3)*3, 1) }
function startOfYear(d){ return new Date(d.getFullYear(), 0, 1) }

export default function DateRangePicker({ minDate, maxDate, from, to, onChange }) {
  const dataMin = minDate || new Date('2000-01-01')
  const dataMax = maxDate || new Date()
  const initialYear = (from && to ? from.getFullYear() : (dataMax.getFullYear()))
  const [year, setYear] = useState(initialYear)

  const monthValue = useMemo(() => {
    if (!from || !to) return ''
    const a = new Date(from.getFullYear(), from.getMonth(), 1)
    const b = new Date(to.getFullYear(), to.getMonth(), 1)
    if (a.getTime() !== b.getTime()) return ''
    return `${a.getFullYear()}-${String(a.getMonth()+1).padStart(2,'0')}`
  }, [from, to])

  function setRange(nextFrom, nextTo, preset){
    if (!nextFrom || !nextTo) return
    if (onChange) onChange({ from: nextFrom, to: nextTo, preset })
  }

  function preset(p) {
    const anchor = new Date(Math.min(Date.now(), (maxDate||new Date()).getTime()))
    const latest = new Date(anchor.getFullYear(), anchor.getMonth(), anchor.getDate())
    if (p === '7d')  return setRange(new Date(latest.getTime()-6*86400000), latest, 'Last 7 days')
    if (p === '30d') return setRange(new Date(latest.getTime()-29*86400000), latest, 'Last 30 days')
    if (p === 'mtd') return setRange(startOfMonth(latest), latest, 'Month to date')
    if (p === 'qtd') return setRange(startOfQuarter(latest), latest, 'Quarter to date')
    if (p === 'ytd') return setRange(startOfYear(latest), latest, 'Year to date')
    if (p === 'all') return setRange(dataMin, dataMax, 'All')
  }

  function onMonthChange(e){
    const v = e.target.value   // YYYY-MM
    if (!v || v.length !== 7) return
    const [y,m] = v.split('-').map(Number)
    const a = new Date(y, m-1, 1)
    const b = endOfMonth(a)
    setRange(a, b, 'Month')
  }

  // Build year options based on data bounds
  const yearOptions = useMemo(() => {
    const y0 = dataMin.getFullYear()
    const y1 = dataMax.getFullYear()
    const ys = []
    for (let y=y0; y<=y1; y++) ys.push(y)
    return ys
  }, [dataMin, dataMax])

  function pickMonth(m) {
    const y = year
    const a = new Date(y, m, 1)
    const b = endOfMonth(a)
    setRange(a, b, 'Month')
  }

  function monthDisabled(m) {
    const a = new Date(year, m, 1)
    const b = endOfMonth(a)
    return (b < dataMin) || (a > dataMax)
  }

  return (
    <div className="controls">
      <div className="controls-row">
        <strong>Range:</strong>
        <div className="btn-group">
          <button className="btn preset" onClick={()=>preset('7d')}>Last 7d</button>
          <button className="btn preset" onClick={()=>preset('30d')}>Last 30d</button>
          <button className="btn preset" onClick={()=>preset('mtd')}>MTD</button>
          <button className="btn preset" onClick={()=>preset('qtd')}>QTD</button>
          <button className="btn preset" onClick={()=>preset('ytd')}>YTD</button>
          <button className="btn preset" onClick={()=>preset('all')}>All</button>
        </div>
      </div>

      <div className="controls-row" style={{gap:12}}>
        <label>From
          <input type="date"
                 min={toInputDate(dataMin)}
                 max={toInputDate(to||dataMax)}
                 value={toInputDate(from)}
                 onChange={e => setRange(fromInputDate(e.target.value), to, 'Custom')} />
        </label>
        <label>To
          <input type="date"
                 min={toInputDate(from||dataMin)}
                 max={toInputDate(dataMax)}
                 value={toInputDate(to)}
                 onChange={e => setRange(from, fromInputDate(e.target.value), 'Custom')} />
        </label>
        <label>Month
          <input type="month" value={monthValue} onChange={onMonthChange} />
        </label>
      </div>

      <div className="controls-row" style={{gap:12, flexWrap:'wrap', alignItems:'center'}}>
        <label>Year
          <select value={year} onChange={e => setYear(parseInt(e.target.value, 10))}>
            {yearOptions.map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </label>
        <div className="btn-group" aria-label="Months">
          {['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].map((lbl, i) => (
            <button key={lbl}
                    className="btn preset"
                    disabled={monthDisabled(i)}
                    onClick={()=>pickMonth(i)}>{lbl}</button>
          ))}
        </div>
      </div>
    </div>
  )
}
