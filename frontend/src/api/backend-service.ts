import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  CheckpointSnapshot,
  EntryRow,
  JoinResultWindow,
  JoinResultVersion,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export type ListQuery = {
  filters?: Record<string, string>
  keyword?: string
  status?: string
  sort?: string
  order?: 'asc' | 'desc'
  page?: number
  size?: number
}

function toQuery(query: ListQuery): string {
  const params = new URLSearchParams()
  Object.entries(query.filters ?? {}).forEach(([key, value]) => {
    if (value) params.set(key, value)
  })
  if (query.keyword) params.set('keyword', query.keyword)
  if (query.status) params.set('status', query.status)
  if (query.sort) params.set('sort', query.sort)
  if (query.order) params.set('order', query.order)
  if (query.page) params.set('page', String(query.page))
  if (query.size) params.set('size', String(query.size))
  return params.toString()
}

export async function listEntries(
  key: string,
  query: ListQuery | Record<string, string> = {},
): Promise<PageResult<EntryRow>> {
  const normalized: ListQuery = 'filters' in query || 'page' in query
    ? (query as ListQuery)
    : { filters: query as Record<string, string> }
  const response = await request(`/api/${key}?${toQuery(normalized)}`)
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
  query: ListQuery | Record<string, string> = {},
): Promise<void> {
  const normalized: ListQuery = 'filters' in query || 'page' in query
    ? (query as ListQuery)
    : { filters: query as Record<string, string> }
  const response = await request(`/api/${key}/export?${toQuery(normalized)}`)
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

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 流关联结果 / 检查点快照（序号8） ----

export type JoinResultPage = PageResult<JoinResultWindow> & {
  key_count: number
  event_count: number
  result_version: number | null
}

export async function listJoinResultWindows(
  joinName: string,
  options: {
    windowKey?: string
    resultVersion?: number | null
    page?: number
    size?: number
  } = {},
): Promise<JoinResultPage> {
  const params = new URLSearchParams({ join_name: joinName })
  if (options.windowKey) params.set('window_key', options.windowKey)
  if (options.resultVersion != null) params.set('result_version', String(options.resultVersion))
  params.set('page', String(options.page ?? 1))
  params.set('size', String(options.size ?? 20))
  const response = await request(`/api/join_stream/results?${params.toString()}`)
  if (!response.ok) throw new Error('关联结果数据读取失败')
  return await response.json()
}

export async function listJoinResultVersions(
  joinName?: string,
): Promise<{ items: JoinResultVersion[]; total: number }> {
  const params = new URLSearchParams()
  if (joinName) params.set('join_name', joinName)
  const response = await request(`/api/join_stream/result-versions?${params.toString()}`)
  if (!response.ok) throw new Error('关联结果版本读取失败')
  return await response.json()
}

export async function listCheckpointSnapshots(options: {
  joinName?: string
  resultVersion?: number | null
  page?: number
  size?: number
} = {}): Promise<PageResult<CheckpointSnapshot>> {
  const params = new URLSearchParams()
  if (options.joinName) params.set('join_name', options.joinName)
  if (options.resultVersion != null) params.set('result_version', String(options.resultVersion))
  params.set('page', String(options.page ?? 1))
  params.set('size', String(options.size ?? 20))
  const response = await request(`/api/checkpoint_mgr/snapshots?${params.toString()}`)
  if (!response.ok) throw new Error('检查点快照读取失败')
  return await response.json()
}

export async function getCheckpointSnapshot(checkpointId: string): Promise<{
  checkpoint_id: string
  config: string
  status: string
  snapshot_ts: number
  join_reference: {
    join_name: string
    result_version: number
    key_count: number
    event_count: number
    matched: number
    windows: { window_start: number; window_end: number }[]
  } | null
  check?: { ok: boolean; issues: string[] }
  result?: { key_count: number; event_count: number }
}> {
  const response = await request(
    `/api/checkpoint_mgr/snapshots/${encodeURIComponent(checkpointId)}`,
  )
  if (!response.ok) throw new Error('检查点快照详情读取失败')
  return await response.json()
}

export type { JoinResultWindow }

