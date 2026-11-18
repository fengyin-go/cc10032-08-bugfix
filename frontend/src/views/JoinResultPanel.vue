<template>
  <section class="join-panel" data-testid="join-result-panel">
    <header class="panel-head">
      <div>
        <h3>关联结果数据</h3>
        <p class="page-desc">
          按窗口查看每个关联键的事件数；同一批事件重复执行只会更新同一个结果版本。
        </p>
      </div>
      <div class="page-actions">
        <label class="panel-field">
          <span>关联配置</span>
          <select
            :value="activeJoin"
            data-testid="join-config-select"
            @change="props.onSelectJoin(($event.target as HTMLSelectElement).value)"
          >
            <option v-for="name in joinNames" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <label class="panel-field">
          <span>结果版本</span>
          <select
            :value="resultVersion ?? ''"
            data-testid="join-version-select"
            @change="changeVersion(($event.target as HTMLSelectElement).value)"
          >
            <option value="">当前版本（v{{ latestVersion || '—' }}）</option>
            <option v-for="version in versions" :key="version.result_version" :value="version.result_version">
              v{{ version.result_version }} · {{ version.key_count }} 键 / {{ version.event_count }} 事件
            </option>
          </select>
        </label>
        <button class="btn" data-testid="join-refresh" type="button" @click="reload">刷新结果</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">结果版本</span>
        <strong class="stat-value" data-testid="join-version">v{{ page.result_version ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">关联键数</span>
        <strong class="stat-value" data-testid="join-key-count">{{ page.key_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">参与事件数</span>
        <strong class="stat-value" data-testid="join-event-count">{{ page.event_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">窗口数</span>
        <strong class="stat-value" data-testid="join-window-count">{{ page.total }}</strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>窗口</th>
          <th>窗口起止</th>
          <th>关联键数</th>
          <th>事件数</th>
          <th>匹配键数</th>
          <th>结果版本</th>
          <th>检查点</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="window in page.items" :key="`${window.window_key}-${window.versions.join('-')}`">
          <td>{{ window.window_key }}</td>
          <td>{{ formatTs(window.window_start) }} ~ {{ formatTs(window.window_end) }}</td>
          <td>{{ window.key_count }}</td>
          <td>{{ window.event_count }}</td>
          <td>{{ window.matched }}</td>
          <td>v{{ window.versions.join(', v') }}</td>
          <td>
            <button
              class="link"
              type="button"
              :data-testid="`join-window-checkpoint-${window.window_key}`"
              @click="locateCheckpoint(window)"
            >
              查看检查点
            </button>
          </td>
        </tr>
        <tr v-if="!page.items.length">
          <td colspan="7" class="empty-state">该关联配置还没有关联结果数据，可先执行关联评估</td>
        </tr>
      </tbody>
    </table>

    <div class="panel-foot">
      <div class="pager">
        <button class="btn ghost" type="button" :disabled="query.page <= 1" @click="changePage(query.page - 1)">
          上一页
        </button>
        <span>第 {{ query.page }} 页 / 共 {{ page.total }} 个窗口</span>
        <button
          class="btn ghost"
          type="button"
          :disabled="query.page * query.size >= page.total"
          @click="changePage(query.page + 1)"
        >
          下一页
        </button>
      </div>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import {
  listJoinResultVersions,
  listJoinResultWindows,
  type JoinResultPage,
} from '@/api/backend-service'
import type { JoinResultVersion, JoinResultWindow } from '@/data/types'

const props = defineProps<{
  joinNames: string[]
  activeJoin: string
  onSelectJoin: (joinName: string) => void
}>()

const route = useRoute()
const router = useRouter()
const page = ref<JoinResultPage>({
  items: [],
  total: 0,
  page: 1,
  size: 20,
  key_count: 0,
  event_count: 0,
  result_version: null,
})
const versions = ref<JoinResultVersion[]>([])
const resultVersion = ref<number | null>(
  route.query.result_version ? Number(route.query.result_version) : null,
)
const windowKey = ref(String(route.query.window_key ?? ''))
const query = ref({ page: 1, size: 20 })
const errorMessage = ref('')

const latestVersion = computed(() => versions.value.at(-1)?.result_version ?? 0)

function formatTs(value: number) {
  if (!value) return '—'
  return new Date(value * 1000).toLocaleString('zh-CN', { hour12: false })
}

function syncUrl() {
  void router.replace({
    path: '/join_stream',
    query: {
      ...route.query,
      ...(props.activeJoin ? { join_name: props.activeJoin } : {}),
      ...(resultVersion.value != null
        ? { result_version: String(resultVersion.value) }
        : {}),
      ...(windowKey.value ? { window_key: windowKey.value } : {}),
    },
  })
}

function changeVersion(raw: string) {
  resultVersion.value = raw ? Number(raw) : null
  query.value.page = 1
  syncUrl()
  void reload()
}

function changePage(next: number) {
  query.value.page = Math.max(1, next)
  void router.replace({
    path: '/join_stream',
    query: {
      ...route.query,
      ...(query.value.page > 1 ? { page: String(query.value.page) } : {}),
    },
  })
  void reload()
}

function locateCheckpoint(window: JoinResultWindow) {
  void router.push({
    path: `/checkpoint_mgr`,
    query: {
      join_name: props.activeJoin,
      result_version: String(window.versions.at(-1) ?? resultVersion.value ?? ''),
      window_key: window.window_key,
    },
  })
}

async function reload() {
  errorMessage.value = ''
  const joinName = props.activeJoin
  if (!joinName) {
    page.value = { ...page.value, items: [], total: 0, key_count: 0, event_count: 0 }
    return
  }
  try {
    const [result, versionPayload] = await Promise.all([
      listJoinResultWindows(joinName, {
        windowKey: windowKey.value || undefined,
        resultVersion: resultVersion.value,
        page: query.value.page,
        size: query.value.size,
      }),
      listJoinResultVersions(joinName),
    ])
    page.value = result
    versions.value = versionPayload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '关联结果数据读取失败'
  }
}

watch(() => props.activeJoin, () => {
  resultVersion.value = null
  windowKey.value = ''
  query.value.page = 1
  void reload()
})

watch(() => route.query.result_version, (value) => {
  const next = value ? Number(value) : null
  if (next !== resultVersion.value) {
    resultVersion.value = next
    void reload()
  }
})

onMounted(() => {
  query.value.page = Number(route.query.page ?? 1) || 1
  void reload()
})
</script>

<style scoped>
.join-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-top: 16px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.panel-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.panel-field select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.panel-foot { display: flex; justify-content: space-between; align-items: center; margin-top: 10px; font-size: 12px; color: var(--muted); }
.pager { display: flex; gap: 8px; align-items: center; }
</style>
