<template>
  <section class="tx-panel" data-testid="tx-replay-panel">
    <header class="tx-panel-head">
      <div>
        <h3>事务包下载与回放</h3>
        <p class="page-desc">
          把已消费的变更事件按事务归并为可回放事件数据包；观察窗结束而 commit 未到的事务保持「未闭合」，
          不会伪装成完整明细。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="tx-package-download" type="button" @click="onDownload">
          下载事务包
        </button>
        <label class="btn file-btn" data-testid="tx-package-replay">
          导入回放
          <input type="file" accept="application/json,.json" hidden @change="onReplayFile" />
        </label>
        <button class="btn ghost" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">已闭合事务（可回放）</span>
        <strong class="stat-value">{{ view?.committed ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未闭合事务（commit 未到）</span>
        <strong class="stat-value tx-open">{{ view?.open ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已回滚事务</span>
        <strong class="stat-value">{{ view?.rolled_back ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">观察窗结束位点</span>
        <strong class="stat-value watermark">
          {{ view ? `${view.watermark.binlog_file}:${view.watermark.binlog_pos}` : '—' }}
        </strong>
      </article>
    </div>

    <p v-if="replayMessage" class="replay-message" :class="{ 'replay-error': replayError }">
      {{ replayMessage }}
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>事务ID</th>
          <th>闭合状态</th>
          <th>成员数</th>
          <th>起始坐标</th>
          <th>提交坐标</th>
          <th>GTID</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="tx in view?.items ?? []" :key="tx.tx_key" :data-testid="`tx-row-${tx.tx_key}`"
            :data-state="tx.state">
          <td>{{ tx.tx_id }}</td>
          <td>
            <span class="tx-state" :class="`tx-state-${tx.state}`">{{ stateLabel(tx.state) }}</span>
          </td>
          <td>{{ tx.member_count }}</td>
          <td>{{ tx.begin.binlog_file }}:{{ tx.begin.binlog_pos }}</td>
          <td>{{ tx.commit ? `${tx.commit.binlog_file}:${tx.commit.binlog_pos}` : '未提交' }}</td>
          <td>{{ tx.gtid || '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`tx-detail-${tx.tx_key}`"
                    @click="openDetail(tx.tx_key)">
              事务详情
            </button>
          </td>
        </tr>
        <tr v-if="!view || !view.items.length">
          <td colspan="7" class="empty-state">暂无归并事务，先消费变更事件</td>
        </tr>
      </tbody>
    </table>

    <div v-if="detail" class="modal-backdrop" data-testid="tx-detail-modal" @click.self="detail = null">
      <div class="modal tx-detail-modal">
        <h3>事务详情 · {{ detail.tx_id }}</h3>
        <dl class="tx-detail-grid">
          <dt>事务身份 tx_key</dt><dd>{{ detail.tx_key }}</dd>
          <dt>闭合状态</dt><dd>{{ stateLabel(detail.state) }}</dd>
          <dt>起始坐标</dt><dd>{{ detail.begin.binlog_file }}:{{ detail.begin.binlog_pos }}</dd>
          <dt>提交坐标</dt>
          <dd>{{ detail.commit ? `${detail.commit.binlog_file}:${detail.commit.binlog_pos}` : '未提交（观察窗结束、commit 未到）' }}</dd>
          <dt>成员数</dt><dd>{{ detail.member_count }}</dd>
          <dt>GTID</dt><dd>{{ detail.gtid || '—' }}</dd>
        </dl>

        <h4>事务成员（来源位点与导出文件一致）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>#</th><th>事件ID</th><th>来源表</th><th>操作</th>
              <th>Binlog文件</th><th>位点</th><th>事件时间</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="member in detail.members ?? []" :key="member.member_key">
              <td>{{ member.seq }}</td>
              <td>{{ member.event_id }}</td>
              <td>{{ member.source_db }}.{{ member.source_table }}</td>
              <td>{{ member.operation }}</td>
              <td>{{ member.binlog_file }}</td>
              <td>{{ member.binlog_pos }}</td>
              <td>{{ member.event_time }}</td>
            </tr>
          </tbody>
        </table>

        <h4>导出批次范围复核</h4>
        <table class="data-table">
          <thead>
            <tr><th>批次</th><th>导出时间</th><th>在包内状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="batch in detail.export_batches ?? []" :key="batch.batch_no">
              <td>第 {{ batch.batch_no }} 轮</td>
              <td>{{ batch.exported_at }}</td>
              <td>{{ packageStateLabel(batch.state_in_package) }}</td>
            </tr>
            <tr v-if="!(detail.export_batches ?? []).length">
              <td colspan="3" class="empty-state">尚未出现在任何导出文件中</td>
            </tr>
          </tbody>
        </table>

        <div class="page-actions modal-actions">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  downloadTxPackage,
  fetchTransactionDetail,
  fetchTransactions,
  replayTxPackageFile,
  type TxnGroup,
  type TxnListView,
} from '@/api/backend-service'

const view = ref<TxnListView | null>(null)
const detail = ref<TxnGroup | null>(null)
const replayMessage = ref('')
const replayError = ref(false)

function stateLabel(state: string): string {
  if (state === 'COMMITTED') return '已闭合'
  if (state === 'OPEN') return '未闭合'
  if (state === 'ROLLED_BACK') return '已回滚'
  return state
}

function packageStateLabel(state: string): string {
  if (state === 'COMMITTED') return '完整明细（COMMITTED）'
  if (state === 'OPEN') return '未闭合标注（OPEN，不参与回放）'
  if (state === 'NOT_INCLUDED') return '未纳入该文件'
  return state
}

async function reload() {
  try {
    view.value = await fetchTransactions()
  } catch (error) {
    replayError.value = true
    replayMessage.value = error instanceof Error ? error.message : '事务视图读取失败'
  }
}

async function onDownload() {
  replayError.value = false
  try {
    await downloadTxPackage()
    replayMessage.value = '事务包已下载，文件内仅完整事务参与回放，未闭合事务单列标注。'
    await reload()
  } catch (error) {
    replayError.value = true
    replayMessage.value = error instanceof Error ? error.message : '事务包下载失败'
  }
}

async function onReplayFile(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  replayError.value = false
  try {
    const result = await replayTxPackageFile(file)
    const applied = result.applied?.length ?? 0
    const skipped = result.skipped?.length ?? 0
    const rejected = result.rejected?.length ?? 0
    replayMessage.value = `${result.message}（新应用 ${applied} 笔、跳过重复 ${skipped} 笔、拒绝 ${rejected} 笔）`
    replayError.value = rejected > 0 || !result.ok
    await reload()
  } catch (error) {
    replayError.value = true
    replayMessage.value = error instanceof Error ? error.message : '回放失败'
  } finally {
    input.value = ''
  }
}

async function openDetail(txKey: string) {
  try {
    detail.value = await fetchTransactionDetail(txKey)
  } catch (error) {
    replayError.value = true
    replayMessage.value = error instanceof Error ? error.message : '事务详情读取失败'
  }
}

onMounted(reload)
</script>
