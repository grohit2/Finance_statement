import React, { useRef, useEffect } from 'react'
import * as d3 from 'd3'

/** Daily spend only (amount < 0), shown as positive bars (abs). */
export default function DailyBarChart({ data, width=920, height=260, onBarClick }) {
  const ref = useRef(null)

  useEffect(() => {
    const svg = d3.select(ref.current).attr('viewBox', `0 0 ${width} ${height}`)
    svg.selectAll('*').remove()

    if (!data || !data.length) return
    // Aggregate spend per calendar day
    const daily = Array.from(
      d3.rollup(
        data,
        v => -d3.sum(v, d => Math.min(0, +d.amount||0)),
        d => d3.timeDay(d.date)  // normalize to midnight
      ),
      ([date, spend]) => ({ date, spend })
    ).sort((a,b) => a.date - b.date)

    if (!daily.length) {
      svg.append('text').attr('x', width/2).attr('y', height/2).attr('text-anchor','middle')
        .text('No spending in range')
      return
    }

    const margin = {top: 16, right: 16, bottom: 40, left: 56}
    const w = width - margin.left - margin.right
    const h = height - margin.top - margin.bottom
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    const x = d3.scaleBand().domain(daily.map(d => +d.date)).range([0, w]).padding(0.1)
    const y = d3.scaleLinear().domain([0, d3.max(daily, d => d.spend)||1]).nice().range([h, 0])
    const color = d3.scaleSequential(d3.interpolateTurbo).domain([0, d3.max(daily, d => d.spend)||1])

    const xAxis = d3.axisBottom(x).tickValues(
      x.domain().filter((_, i) => i % Math.ceil(daily.length/10) === 0)
    ).tickFormat(t => d3.timeFormat('%b %d')(new Date(+t)))
    const yAxis = d3.axisLeft(y).ticks(5).tickFormat(d => `$${d3.format(',.0f')(d)}`)

    g.append('g').attr('transform', `translate(0,${h})`).call(xAxis)
    g.append('g').call(yAxis)

    g.selectAll('rect').data(daily).enter().append('rect')
      .attr('x', d => x(+d.date))
      .attr('y', d => y(d.spend))
      .attr('width', x.bandwidth())
      .attr('height', d => h - y(d.spend))
      .attr('fill', d => color(d.spend))
      .attr('role','img')
      .attr('aria-label', d => `Spend on ${d3.timeFormat('%b %d, %Y')(d.date)} $${d3.format(',.2f')(d.spend)}`)
      .style('cursor', onBarClick ? 'pointer' : 'default')
      .on('click', (e, d) => onBarClick && onBarClick(d.date))

    // Zero-line if needed
    g.append('line').attr('x1',0).attr('x2',w).attr('y1',h+0.5).attr('y2',h+0.5).attr('stroke','#bbb')
  }, [data, width, height, onBarClick])

  return <svg ref={ref} aria-label="Daily spending bar chart" />
}
