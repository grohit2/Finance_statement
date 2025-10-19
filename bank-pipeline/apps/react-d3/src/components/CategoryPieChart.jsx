import React, { useRef, useEffect } from 'react'
import * as d3 from 'd3'

/** Spend by category pie (amount < 0 only). */
export default function CategoryPieChart({ data, width=420, height=260, onSliceClick }) {
  const ref = useRef(null)

  useEffect(() => {
    const svg = d3.select(ref.current).attr('viewBox', `0 0 ${width} ${height}`)
    svg.selectAll('*').remove()
    if (!data || !data.length) return

    const cats = Array.from(
      d3.rollup(data, v => -d3.sum(v, d => Math.min(0, +d.amount||0)), d => d.category ?? 'Uncategorized'),
      ([category, total]) => ({ category, total })
    ).filter(d => d.total > 0)
     .sort((a,b) => b.total - a.total)

    if (!cats.length) {
      svg.append('text').attr('x', width/2).attr('y', height/2).attr('text-anchor','middle').text('No spending')
      return
    }

    const r = Math.min(width, height) / 2 - 8
    const g = svg.append('g').attr('transform', `translate(${width/2},${height/2})`)
    const color = d3.scaleOrdinal(d3.schemeTableau10).domain(cats.map(d => d.category))

    const pie = d3.pie().value(d => d.total)
    const arc = d3.arc().innerRadius(0).outerRadius(r)
    const label = d3.arc().innerRadius(r*0.6).outerRadius(r*0.9)

    const arcs = g.selectAll('path').data(pie(cats)).enter()
    arcs.append('path')
      .attr('d', arc)
      .attr('fill', d => color(d.data.category))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1)
      .style('cursor', onSliceClick ? 'pointer' : 'default')
      .on('click', (_, d) => onSliceClick && onSliceClick(d.data.category))
      .append('title').text(d => `${d.data.category}: $${d3.format(',.0f')(d.data.total)}`)

    // Labels for larger slices
    g.selectAll('text').data(pie(cats)).enter().append('text')
      .attr('transform', d => `translate(${label.centroid(d)})`)
      .attr('text-anchor','middle')
      .attr('font-size', 11)
      .attr('fill', '#222')
      .text(d => (d.data.total / d3.sum(cats, c => c.total) > 0.07) ? d.data.category : '')
  }, [data, width, height, onSliceClick])

  return <svg ref={ref} aria-label="Spend by category pie chart" />
}
