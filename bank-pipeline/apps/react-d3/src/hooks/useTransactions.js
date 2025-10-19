import * as d3 from 'd3'

export async function loadShareIndex() {
  const rows = await d3.csv('/data/share_index.csv', d3.autoType)
  return rows
}

export async function loadTransactions(ownerProfileId) {
  const url = `/data/transactions/profile=${ownerProfileId}/transactions.csv`
  const rows = await d3.csv(url, d3.autoType)
  return rows.map(r => ({
    ...r,
    date: new Date(r.post_date),
    amount: +r.amount
  }))
}
