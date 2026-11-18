/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult<T = EntryRow> = {
  items: T[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type JoinResultWindow = {
  join_name?: string
  window_key: string
  window_start: number
  window_end: number
  key_count: number
  event_count: number
  matched: number
  versions: number[]
}

export type JoinResultVersion = {
  result_version: number
  join_name: string
  window_keys: string[]
  key_count: number
  event_count: number
  matched: number
  signature: string
  snapshot_ts: number
}

export type CheckpointSnapshot = {
  checkpoint_id: string
  id: number | string
  config: string
  status: string
  pruned: boolean
  snapshot_ts: number
  join_name: string | null
  result_version: number | null
  key_count: number
  event_count: number
  windows: { window_start: number; window_end: number }[]
  complete: boolean | null
  issues: string[]
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
