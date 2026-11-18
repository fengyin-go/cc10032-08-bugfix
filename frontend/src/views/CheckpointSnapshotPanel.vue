<template>
  <section class="snapshot-panel" data-testid="checkpoint-snapshot-panel">
    <header class="panel-head">
      <div>
        <h3>检查点快照</h3>
        <p class="page-desc">
          每个快照引用一个完整的关联结果版本；键数、事件数和窗口边界对不上时标记为不完整。
        </p>
      </div>
      <div class="page-actions">
        <label class="panel-field">
          <span>关联配置</span>
          <select
            :value="joinName"
            data-testid="checkpoint-join-select"
            @change="changeJoin(($event.target as HTMLSelectElement).value)"
          >
            <option value="">全部关联配置</option>
            <option v-for="name in joinNames" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <label class="panel-field">
          <span>结果版本</span>
          <input
            :value="resultVersion ?? ''"
            data-testid="checkpoint-version-input"
            placeholder="全部版本"
            @change="changeVersion(($event.target as HTMLInputElement).value)"
          />
        </label>
      </div>
    </header>

    <table class="data-table">
      <thead>
        <tr>
          <th>检查点</th>
          <th>配置</th>
          <th>关联配置</th>
          <th>结果版本</th>
          <th>键数</th>
          <th>事件数</th>
          <th>窗口边界</th>
          <th>完整性</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="snapshot in page.items"
          :key="snapshot.checkpoint_id"
          :data-testid="`snapshot-${snapshot.checkpoint_id}`"
        >
          <td>{{ snapshot.checkpoint_id }}</td>
          <td>{{ snapshot.config }}</td>
          <td>{{ snapshot.join_name ?? '—' }}</td>
          <td>{{ snapshot.result_version != null ? `v${snapshot.result_version}` : '—' }}</td>
          <td data-testid="snapshot-key-count">{{ snapshot.key_count }}</td>
          <td data-testid="snapshot-event-count">{{ snapshot.event_count }}</td>
          <td>{{ describeWindows(snapshot.windows) }}</td>
          <td>
            <span v-if="snapshot.complete === null">—</span>
            <span v-else-if="snapshot.complete" class="tag ok">完整</span>
            <span v-else class="tag bad" :title="snapshot.issues.join('；')">不完整</span>
          </td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`locate-join-${snapshot.checkpoint_id}`"
              @click="locateJoinResult(snapshot)"
            >
              定位关联结果
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`restore-${snapshot.checkpoint_id}`"
              @click="restore(snapshot)"
            >
              校验恢复
            </button>
          </td>
        </tr>
        <tr v-if="!page.items.length">
          <td colspan="9" class="empty-state">当前筛选条件下没有检查点快照</td>
        </tr>
      </tbody>
    </table>

    <div class="panel-foot">
      <div class="pager">
        <button class="btn ghost" type="button" :disabled="page.page <= 1" @click="goToPage(page.page - 1)">
          上一页
        </button>
        <span data-testid="checkpoint-pager">
          第 {{ page.page }} 页 / 共 {{ page.total }} 个快照
        </span>
        <button
          class="btn ghost"
          type="button"
          :disabled="page.page * page.size >= page.total"
          @click="goToPage(page.page + 1)"
        >
          下一页
        </button>
      </div>
      <span v-if="message" data-testid="checkpoint-message">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { getCheckpointSnapshot, listCheckpointSnapshots } from '@/api/backend-service'
import type { CheckpointSnapshot, PageResult } from '@/data/types'

const props = defineProps<{ joinNames: string[] }>()

const route = useRoute()
const router = useRouter()
const joinName = ref(String(route.query.join_name ?? ''))
const resultVersion = ref<number | null>(
  route.query.result_version ? Number(route.query.result_version) : null,
)
const page = ref<PageResult<CheckpointSnapshot>>({ items: [], total: 0, page: 1, size: 20 })
const errorMessage = ref('')
const message = ref('')

function describeWindows(windows: { window_start: number; window_end: number }[]) {
  if (!windows.length) return '—'
  return windows
    .map((w) => `${new Date(w.window_start * 1000).toLocaleTimeString('zh-CN', { hour12: false })}~${new Date(w.window_end * 1000).toLocaleTimeString('zh-CN', { hour12: false })}`)
    .join('；')
}

function syncQuery(nextPage?: number) {
  void router.replace({
    path: '/checkpoint_mgr',
    query: {
      ...route.query,
      ...(joinName.value ? { join_name: joinName.value } : {}),
      ...(resultVersion.value != null ? { result_version: String(resultVersion.value) } : {}),
      ...(nextPage && nextPage > 1 ? { page: String(nextPage) } : {}),
    },
  })
}

async function reload(targetPage = page.value.page) {
  errorMessage.value = ''
  try {
    page.value = await listCheckpointSnapshots({
      joinName: joinName.value || undefined,
      resultVersion: resultVersion.value,
      page: targetPage,
      size: 20,
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点快照读取失败'
  }
}

function changeJoin(value: string) {
  joinName.value = value
  syncQuery()
  void reload(1)
}

function changeVersion(raw: string) {
  resultVersion.value = raw ? Number(raw) : null
  syncQuery()
  void reload(1)
}

function goToPage(next: number) {
  syncQuery(next)
  void reload(next)
}

function locateJoinResult(snapshot: CheckpointSnapshot) {
  void router.push({
    path: '/join_stream',
    query: {
      ...(snapshot.join_name ? { join_name: snapshot.join_name } : {}),
      ...(snapshot.result_version != null
        ? { result_version: String(snapshot.result_version) }
        : {}),
      ...(snapshot.windows.length
        ? { window_key: windowKeyOf(snapshot.windows[0]) }
        : {}),
    },
  })
}

function windowKeyOf(window: { window_start: number; window_end: number }) {
  return `w_${Math.trunc(window.window_start)}_${Math.trunc(window.window_end)}`
}

async function restore(snapshot: CheckpointSnapshot) {
  message.value = ''
  try {
    const detail = await getCheckpointSnapshot(snapshot.checkpoint_id)
    const check = detail.check
    if (check && !check.ok) {
      message.value = `快照 ${snapshot.checkpoint_id} 引用的结果版本不完整：${check.issues.join('；')}`
      return
    }
    message.value = `快照 ${snapshot.checkpoint_id} 引用的结果版本完整（${detail.result?.key_count ?? snapshot.key_count} 键 / ${detail.result?.event_count ?? snapshot.event_count} 事件），可安全恢复`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点快照校验失败'
  }
}

watch(() => route.query.join_name, (value) => {
  const next = String(value ?? '')
  if (next !== joinName.value) {
    joinName.value = next
    void reload(1)
  }
})

watch(() => route.query.result_version, (value) => {
  const next = value ? Number(value) : null
  if (next !== resultVersion.value) {
    resultVersion.value = next
    void reload(1)
  }
})

onMounted(() => {
  void reload(route.query.page ? Number(route.query.page) : 1)
})
</script>

<style scoped>
.snapshot-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-top: 16px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h3 { margin: 0; font-size: 15px; }
.panel-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.panel-field select, .panel-field input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; }
.panel-foot { display: flex; justify-content: space-between; align-items: center; margin-top: 10px; font-size: 12px; color: var(--muted); }
.pager { display: flex; gap: 8px; align-items: center; }
.tag { padding: 2px 8px; border-radius: 999px; font-size: 12px; }
.tag.ok { background: #e7f6ec; color: #157347; }
.tag.bad { background: #fdecea; color: #b42318; }
</style>
