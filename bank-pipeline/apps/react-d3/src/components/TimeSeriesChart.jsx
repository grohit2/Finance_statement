import React, { useRef, useEffect } from 'react'
import * as d3 from 'd3'

export default function TimeSeriesChart({ data, width=920, height=300 }) {
  const ref = useRef(null)

  useEffect(() => {
    const svg = d3.select(ref.current).attr('viewBox', `0 0 ${width} ${height}`)
    svg.selectAll('*').remove()
    if (!data || !data.length) return

    const margin = {top: 16, right: 24, bottom: 36, left: 56}
    const w = width - margin.left - margin.right
    const h = height - margin.top - margin.bottom
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    const daily = Array.from(
      d3.rollup(data, v => d3.sum(v, d => d.amount), d => d3.timeDay(d.date)),
      ([date, total]) => ({date, total})
    ).sort((a,b) => a.date - b.date)

    const x = d3.scaleTime().domain(d3.extent(daily, d => d.date)).range([0, w])
    const y = d3.scaleLinear().domain(d3.extent(daily, d => d.total)).nice().range([h, 0])

    g.append('g').attr('transform', `translate(0,${h})`).call(d3.axisBottom(x))
    g.append('g').call(d3.axisLeft(y).tickFormat(d => `$${d.toFixed(0)}`))

    const line = d3.line().x(d => x(d.date)).y(d => y(d.total)).curve(d3.curveMonotoneX)
    g.append('path').datum(daily).attr('fill','none').attr('stroke','#2563eb').attr('stroke-width',2).attr('d', line)

    if (y.domain()[0] < 0 && y.domain()[1] > 0) {
      g.append('line').attr('x1',0).attr('x2',w).attr('y1',y(0)).attr('y2',y(0))
        .attr('stroke','#aaa').attr('stroke-dasharray','4 4')
    }
  }, [data, width, height])

  return <svg ref={ref} role="img" aria-label="Daily net spend line chart" />
}
