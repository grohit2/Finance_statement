import React, { useMemo } from 'react'
import DailyBarChart from './DailyBarChart.jsx'
import * as d3 from 'd3'

function formatMoney(n){ return (n<0? '-' : '') + '$' + Math.abs(n).toLocaleString(undefined, {maximumFractionDigits:2}) }
function sameDay(a,b){ return a.getFullYear()===b.getFullYear() && a.getMonth()===b.getMonth() && a.getDate()===b.getDate() }

export default function Drilldown({ data, type, value, from, to, onClose }) {
  const { title, subset, dailySpend, totalSpend } = useMemo(() => {
    if (!data || !data.length) return { title:'', subset:[], dailySpend:[], totalSpend:0 }
    let subset = []
    let title = ''
    if (type === 'category') {
      title = `Category: ${value}`
      subset = data.filter(r => r.category === value)
    } else if (type === 'day' && value instanceof Date) {
      title = `Day: ${value.toLocaleDateString()}`
      subset = data.filter(r => sameDay(new Date(r.date), value))
    }
    const totalSpend = -d3.sum(subset, r => Math.min(0, +r.amount||0))
    return { title, subset, dailySpend: subset, totalSpend }
  }, [data, type, value])

  if (!type || !value || !subset.length) return null

  return (
    <div className="card">
      <div style={{display:'flex', justifyContent:'space-between', alignItems:'center'}}>
        <h3 style={{margin:'4px 0 12px'}}>{title}</h3>
        <button className="btn" onClick={onClose}>Close</button>
      </div>
      {type === 'category' && (
        <>
          <p style={{margin:'4px 0 12px', color:'#444'}}>Total spending in range: <b>{formatMoney(totalSpend)}</b></p>
          <DailyBarChart data={dailySpend} height={180} />
        </>
      )}

      <div style={{overflowX:'auto', marginTop:12}}>
        <table className="tx-table">
          <thead>
            <tr>
              <th>Date</th><th>Description</th><th>Category</th><th>Merchant</th><th style={{textAlign:'right'}}>Amount</th>
            </tr>
          </thead>
          <tbody>
            {subset.sort((a,b)=> new Date(b.date)-new Date(a.date)).map((r, i) => (
              <tr key={i}>
                <td>{new Date(r.date).toLocaleDateString()}</td>
                <td>{r.name || r.clean_desc || r.description}</td>
                <td>{r.category || '—'}</td>
                <td>{r.merchant || '—'}</td>
                <td style={{textAlign:'right', color: (+r.amount)<0 ? '#b91c1c' : '#065f46' }}>{formatMoney(+r.amount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
