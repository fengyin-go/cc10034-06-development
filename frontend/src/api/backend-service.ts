import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

// ---------------------------------------------------------------------------
// 变更事件 · 事务包下载 / 导入回放
// ---------------------------------------------------------------------------

export type TxnMember = {
  member_key: string
  seq: number
  event_id: string
  source_db: string
  source_table: string
  operation: string
  binlog_file: string
  binlog_pos: number
  event_time: string
  before_image: Record<string, unknown> | null
  after_image: Record<string, unknown> | null
  gtid: string
  raw_format: string
}

export type TxnGroup = {
  tx_key: string
  tx_id: string
  state: 'OPEN' | 'COMMITTED' | 'ROLLED_BACK'
  member_count: number
  begin: { binlog_file: string; binlog_pos: number }
  commit: { binlog_file: string; binlog_pos: number } | null
  gtid: string
  first_seen: string
  last_seen: string
  members?: TxnMember[]
  export_batches?: { batch_no: number; exported_at: string; included: boolean; state_in_package: string }[]
}

export type TxnListView = {
  items: TxnGroup[]
  total: number
  committed: number
  open: number
  rolled_back: number
  watermark: { binlog_file: string; binlog_pos: number }
}

export type ReplayResult = {
  ok: boolean
  message: string
  applied?: { tx_key: string; tx_id: string; member_count: number; members_written: number }[]
  skipped?: { tx_key: string; tx_id: string; reason: string; first_applied_at?: string }[]
  rejected?: { tx_key: string; tx_id: string; reason: string }[]
}

export async function fetchTransactions(status?: string): Promise<TxnListView> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const response = await request(`/api/change_event/transactions${query}`)
  if (!response.ok) throw new Error('事务明细读取失败')
  return await response.json()
}

export async function fetchTransactionDetail(txKey: string): Promise<TxnGroup> {
  const response = await request(`/api/change_event/transactions/${encodeURIComponent(txKey)}`)
  if (!response.ok) throw new Error('事务详情读取失败')
  return await response.json()
}

export async function fetchExportBatches(): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const response = await request('/api/change_event/export-batches')
  if (!response.ok) throw new Error('导出批次读取失败')
  return await response.json()
}

export async function downloadTxPackage(): Promise<void> {
  const response = await request('/api/change_event/tx-package/download')
  if (!response.ok) throw new Error('事务包下载失败')
  const blob = await response.blob()
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `change_event_tx_replay_${Date.now()}.json`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function replayTxPackageFile(file: File): Promise<ReplayResult> {
  const text = await file.text()
  let packagePayload: Record<string, unknown>
  try {
    packagePayload = JSON.parse(text) as Record<string, unknown>
  } catch {
    throw new Error('文件不是合法的 JSON 事务包')
  }
  const response = await request('/api/change_event/replay', {
    method: 'POST',
    body: JSON.stringify({ package: packagePayload }),
  })
  return (await response.json()) as ReplayResult
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
