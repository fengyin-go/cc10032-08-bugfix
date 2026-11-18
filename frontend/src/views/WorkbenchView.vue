<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="applyFilters">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <label class="filter-item">
        <span>关键词</span>
        <input v-model="keyword" data-testid="keyword" :placeholder="`按${meta.fields[0]}关键词检索`" />
      </label>
      <label class="filter-item">
        <span>状态</span>
        <select v-model="status" data-testid="status-filter">
          <option value="">全部状态</option>
          <option v-for="item in meta.statuses" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>排序字段</span>
        <select v-model="sortField" data-testid="sort-field">
          <option value="">默认顺序</option>
          <option v-for="field in meta.fields" :key="field" :value="field">{{ field }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>排序方向</span>
        <select v-model="sortOrder" data-testid="sort-order">
          <option value="asc">升序</option>
          <option value="desc">降序</option>
        </select>
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div class="page-actions" style="justify-content: space-between; margin-top: 10px">
      <div class="page-actions">
        <button class="btn ghost" data-testid="page-prev" type="button" :disabled="page <= 1" @click="goToPage(page - 1)">
          上一页
        </button>
        <span data-testid="page-info">第 {{ page }} 页 / 每页 {{ pageSize }} 条 / 共 {{ total }} 条</span>
        <button
          class="btn ghost"
          data-testid="page-next"
          type="button"
          :disabled="page * pageSize >= total"
          @click="goToPage(page + 1)"
        >
          下一页
        </button>
      </div>
      <span class="page-desc">当前筛选条件会写入地址栏，跨页面返回时保持同一结果</span>
    </div>

    <JoinResultPanel
      v-if="meta.key === 'join_stream'"
      :join-names="joinNames"
      :active-join="activeJoinName"
      :on-select-join="selectJoin"
    />
    <CheckpointSnapshotPanel
      v-if="meta.key === 'checkpoint_mgr'"
      :join-names="joinNames"
    />

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import CheckpointSnapshotPanel from '@/views/CheckpointSnapshotPanel.vue'
import JoinResultPanel from '@/views/JoinResultPanel.vue'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const route = useRoute()
const router = useRouter()

const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const keyword = ref('')
const status = ref('')
const sortField = ref('')
const sortOrder = ref<'asc' | 'desc'>('asc')
const page = ref(1)
const pageSize = ref(20)
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})

const activeJoinName = computed(() => {
  if (props.moduleKey === 'join_stream') {
    return String(route.query.join_name ?? filters.value[meta.fields[0]] ?? '')
  }
  return String(route.query.join_name ?? '')
})
const joinCatalog = ref<string[]>([])
const joinNames = computed(() => {
  if (props.moduleKey === 'join_stream' || props.moduleKey === 'checkpoint_mgr') {
    return joinCatalog.value
  }
  return []
})

const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((item) => ({
    status: item,
    count: rows.value.filter((row) => String(row.status) === item).length,
  })),
)

function queryState() {
  return {
    filters: { ...filters.value },
    keyword: keyword.value,
    status: status.value,
    sort: sortField.value,
    order: sortOrder.value,
    page: page.value,
    size: pageSize.value,
  }
}

function syncUrl() {
  const params: Record<string, string> = {}
  Object.entries(filters.value).forEach(([key, value]) => {
    if (value) params[key] = value
  })
  if (keyword.value) params.keyword = keyword.value
  if (status.value) params.status = status.value
  if (sortField.value) params.sort = sortField.value
  if (sortField.value && sortOrder.value !== 'asc') params.order = sortOrder.value
  if (page.value > 1) params.page = String(page.value)
  if (props.moduleKey === 'join_stream' && activeJoinName.value) {
    params.join_name = activeJoinName.value
  }
  // 保留子面板（关联结果版本、快照分页）写入的其它查询参数，跨页面定位不丢条件
  const ownedKeys = new Set([
    ...filterFields, 'keyword', 'status', 'sort', 'order', 'page',
  ])
  if (props.moduleKey === 'join_stream') ownedKeys.add('join_name')
  Object.entries(route.query).forEach(([key, value]) => {
    if (!ownedKeys.has(key) && typeof value === 'string' && value) {
      params[key] = value
    }
  })
  const current = route.query
  const same = Object.entries(params).every(([key, value]) => current[key] === value)
    && Object.keys(current).every((key) => key in params)
  if (!same) void router.replace({ path: route.path, query: params })
}

function restoreFromUrl() {
  filters.value = Object.fromEntries(
    meta.fields
      .slice(0, 3)
      .filter((field) => typeof route.query[field] === 'string')
      .map((field) => [field, String(route.query[field])]),
  )
  keyword.value = String(route.query.keyword ?? '')
  status.value = String(route.query.status ?? '')
  sortField.value = String(route.query.sort ?? '')
  sortOrder.value = route.query.order === 'desc' ? 'desc' : 'asc'
  page.value = Number(route.query.page ?? 1) || 1
}

function resetFilters() {
  filters.value = {}
  keyword.value = ''
  status.value = ''
  sortField.value = ''
  sortOrder.value = 'asc'
  page.value = 1
  void applyFilters()
}

function goToPage(next: number) {
  page.value = Math.max(1, next)
  void reload()
}

function applyFilters() {
  page.value = 1
  void reload()
}

function selectJoin(joinName: string) {
  filters.value[meta.fields[0]] = joinName
  page.value = 1
  void reload()
}

async function loadJoinNames(): Promise<string[]> {
  try {
    const payload = await listEntries('join_stream', { page: 1, size: 200 })
    return payload.items
      .map((row) => String(row['配置名称'] ?? ''))
      .filter(Boolean)
  } catch {
    return []
  }
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, queryState())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    if (
      props.moduleKey === 'join_stream'
      && !activeJoinName.value
      && filters.value[meta.fields[0]] === undefined
    ) {
      // 流关联页先确定当前关联配置，再按该配置读取列表和结果，避免默认全量
      joinCatalog.value = await loadJoinNames()
      if (joinCatalog.value.length) {
        filters.value[meta.fields[0]] = joinCatalog.value[0]
      }
    }
    const payload = await listEntries(meta.key, queryState())
    rows.value = payload.items
    total.value = payload.total
    page.value = payload.page
    if (props.moduleKey === 'checkpoint_mgr') {
      joinCatalog.value = await loadJoinNames()
    }
    syncUrl()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(() => {
  restoreFromUrl()
  void reload()
})
</script>
