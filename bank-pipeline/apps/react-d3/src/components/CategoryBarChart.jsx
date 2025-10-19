import React, { useRef, useEffect } from 'react'
import * as d3 from 'd3'

export default function CategoryBarChart({ data, width=920, height=300, onBarClick }) {
  const ref = useRef(null)

  useEffect(() => {
    const svg = d3.select(ref.current).attr('viewBox', `0 0 ${width} ${height}`)
    svg.selectAll('*').remove()
    if (!data || !data.length) return

    const margin = {top: 16, right: 24, bottom: 64, left: 100}
    const w = width - margin.left - margin.right
    const h = height - margin.top - margin.bottom
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    // Spend only (amount < 0), aggregated by category (positive totals)
    const cats = Array.from(
      d3.rollup(
        data,
        v => -d3.sum(v, d => Math.min(0, +d.amount||0)),
        d => d.category ?? 'Uncategorized'
      ),
      ([category, total]) => ({ category, total })
    ).filter(d => d.total > 0)
     .sort((a,b) => b.total - a.total)

    if (!cats.length) {
      svg.append('text').attr('x', width/2).attr('y', height/2).attr('text-anchor','middle').text('No spending in range')
      return
    }

    const x = d3.scaleLinear().domain([0, d3.max(cats, d => d.total)]).nice().range([0, w])
    const y = d3.scaleBand().domain(cats.map(d => d.category)).range([0, h]).padding(0.15)
    const color = d3.scaleSequential(d3.interpolatePlasma).domain([0, d3.max(cats, d => d.total)||1])

    g.append('g').attr('transform', `translate(0,${h})`)
      .call(d3.axisBottom(x).ticks(5).tickFormat(d => `$${d3.format(',.0f')(d)}`))
    g.append('g').call(d3.axisLeft(y))

    const bars = g.selectAll('rect').data(cats).enter().append('rect')
      .attr('x', 0).attr('y', d => y(d.category))
      .attr('width', d => x(d.total)).attr('height', y.bandwidth())
      .attr('fill', d => color(d.total))
      .style('cursor', onBarClick ? 'pointer' : 'default')
      .on('click', (_, d) => onBarClick && onBarClick(d.category))

    // value labels
    g.selectAll('.bar-label').data(cats).enter().append('text')
      .attr('x', d => x(d.total) + 6).attr('y', d => (y(d.category) + y.bandwidth()/2)+4)
      .attr('fill', '#333').attr('font-size', 12)
      .text(d => `$${d3.format(',.0f')(d.total)}`)
  }, [data, width, height, onBarClick])

  return <svg ref={ref} role="img" aria-label="Spend by category bar chart" />
}
