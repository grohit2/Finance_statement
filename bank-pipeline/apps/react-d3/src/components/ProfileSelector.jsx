import React from 'react'

export default function ProfileSelector({ viewer, owners, owner, onChangeViewer, onChangeOwner }) {
  const viewerOptions = [...new Set(owners.map(o => o.viewer_profile_id))]
  const ownerOptions = owners.filter(o => o.viewer_profile_id===viewer)
                            .map(o => o.owner_profile_id)
                            .filter((v,i,a)=>a.indexOf(v)===i)

  return (
    <div style={{display:'flex', gap:16, flexWrap:'wrap', alignItems:'center'}}>
      <label>Viewer:
        <select value={viewer} onChange={e => onChangeViewer(e.target.value)}>
          {viewerOptions.map(v => <option key={v} value={v}>{v}</option>)}
        </select>
      </label>
      <label>Owner:
        <select value={owner} onChange={e => onChangeOwner(e.target.value)}>
          {ownerOptions.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
      </label>
    </div>
  )
}
