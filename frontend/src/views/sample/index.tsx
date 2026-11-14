import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/sample'
const columns = [
  "配置编号",
  "数据源名称",
  "采样算法",
  "样本比例",
  "随机种子",
  "分层字段",
  "输出表名",
  "配置状态",
  "完成位置"
]
const actions = [
  "启用采样",
  "暂停采样",
  "关闭采样"
]

type Row = Record<string, string | number | null>

type Batch = {
  序号: number
  分层字段: string
  层内批次: number
  层内批数: number
  随机种子断点: number
  样本起点: number
  样本终点: number
  计划行数: number
  已采样行数: number
  状态: string
  写入次数: number
  完成时间: string | null
  失败原因: string | null
}

type RunDetail = {
  id: number
  运行编号: string
  配置编号: string
  输出表名: string
  版本: number
  状态: string
  样本比例: number
  随机种子: number
  分层字段: string[]
  源行数: number
  完成位置: string
  批次: Batch[]
  消息: string
}

type RunSummary = {
  id: number
  运行编号: string
  版本: number
  状态: string
  样本比例: number
  随机种子: number
  分层字段: string[]
  批次数: number
  完成位置: string
  结果已清理: boolean
}

type OutputVersion = {
  版本: number
  运行编号: string
  状态: string
  样本比例: number
  行数: number
  分层行数: Record<string, number>
  结果已清理: boolean
}

type OutputOverview = {
  输出表名: string
  总行数: number
  版本: OutputVersion[]
}

function splitLayers(raw: string | number | null | undefined): string[] {
  return String(raw ?? '')
    .split(/[,，]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function batchTagClass(status: string): string {
  if (status === '已完成') return 'tag ok'
  if (status === '失败') return 'tag fail'
  return 'tag wait'
}

export default function SampleView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const [activeRow, setActiveRow] = useState<Row | null>(null)
  const [runs, setRuns] = useState<RunSummary[]>([])
  const [currentRun, setCurrentRun] = useState<RunDetail | null>(null)
  const [output, setOutput] = useState<OutputOverview | null>(null)
  const [selectedLayers, setSelectedLayers] = useState<string[]>([])
  const [failAt, setFailAt] = useState('')
  const [sourceRows, setSourceRows] = useState('')
  const [newRatio, setNewRatio] = useState('')

  const stats = useMemo(() => {
    const enabled = rows.filter((row) => row.status === '已启用').length
    const paused = rows.filter((row) => row.status === '已暂停').length
    return [
      { label: '采样配置总数', value: total },
      { label: '已启用采样', value: enabled },
      { label: '已暂停采样', value: paused },
    ]
  }, [rows, total])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('采样配置列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据采样管理列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  const loadRuns = useCallback(async (row: Row) => {
    const response = await request(`${ENDPOINT}/${row.id}/runs`)
    if (!response.ok) return
    const payload = await response.json()
    setRuns(payload.items ?? [])
  }, [])

  const loadOutput = useCallback(async (row: Row) => {
    const response = await request(`${ENDPOINT}/${row.id}/output`)
    if (!response.ok) {
      setOutput(null)
      return
    }
    const payload = await response.json()
    setOutput(payload.overview ?? null)
  }, [])

  async function openRunner(row: Row) {
    setNotice('')
    setErrorMessage('')
    setActiveRow(row)
    setCurrentRun(null)
    setSelectedLayers(splitLayers(row["分层字段"]))
    setNewRatio(String(row["样本比例"] ?? ''))
    setFailAt('')
    await Promise.all([loadRuns(row), loadOutput(row)])
  }

  async function showRun(runId: number) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/runs/${runId}`)
      if (!response.ok) {
        throw new Error('采样运行明细读取失败')
      }
      setCurrentRun(await response.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采样运行明细读取失败')
    }
  }

  async function submitRun() {
    if (!activeRow) return
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${activeRow.id}/runs`, {
        method: 'POST',
        body: JSON.stringify({
          分层字段: selectedLayers,
          源行数: sourceRows,
          模拟中断批次: failAt,
        }),
      })
      const payload = await response.json()
      if (!payload.ok) {
        throw new Error(payload.message || '采样运行提交失败')
      }
      setNotice(payload.message)
      setCurrentRun(payload.entry)
      await Promise.all([loadRuns(activeRow), loadOutput(activeRow), reload()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采样运行提交失败')
    }
  }

  async function resumeRun() {
    if (!activeRow || !currentRun) return
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/runs/${currentRun.id}/resume`, {
        method: 'POST',
        body: JSON.stringify({ 模拟中断批次: failAt }),
      })
      const payload = await response.json()
      if (!payload.ok) {
        throw new Error(payload.message || '采样运行续跑失败')
      }
      setNotice(payload.message)
      setCurrentRun(payload.entry)
      await Promise.all([loadRuns(activeRow), loadOutput(activeRow), reload()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采样运行续跑失败')
    }
  }

  async function saveRatio() {
    if (!activeRow) return
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${activeRow.id}/settings`, {
        method: 'POST',
        body: JSON.stringify({ 样本比例: newRatio }),
      })
      const payload = await response.json()
      if (!payload.ok) {
        throw new Error(payload.message || '样本比例更新失败')
      }
      setNotice(payload.message)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '样本比例更新失败')
    }
  }

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('数据采样管理动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据采样管理操作失败')
    }
  }

  function toggleLayer(layer: string) {
    setSelectedLayers((prev) =>
      prev.includes(layer) ? prev.filter((item) => item !== layer) : [...prev, layer],
    )
  }

  return (
    <section className="page" data-module="sample">
      <header className="page-head">
        <div>
          <h2>数据采样管理管理</h2>
          <p className="page-desc">维护数据采样的算法选择、比例控制与分层配置，为数据采样管理提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('采样配置登记入口尚未接入审批流')}>
            登记采样配置
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据采样管理清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
                <button className="link" type="button" onClick={() => void openRunner(row)}>
                  采样运行
                </button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无数据采样管理数据，可先登记采样配置</td>
            </tr>
          )}
        </tbody>
      </table>

      {activeRow && (
        <section className="run-panel">
          <header className="panel-head">
            <div>
              <h3>分批采样运行：{String(activeRow["配置编号"])}</h3>
              <p className="page-desc">
                输出表「{String(activeRow["输出表名"] ?? '—')}」；大样本按分层字段批量切分，每批 500 行，
                断点粒度为分层字段级随机种子，输出表保留最近 3 个版本。
              </p>
            </div>
            <button className="btn ghost" type="button" onClick={() => { setActiveRow(null); setCurrentRun(null) }}>
              收起面板
            </button>
          </header>

          <div className="panel-grid">
            <div className="panel-block">
              <h4>整组提交（多选分层，一次提交）</h4>
              <div className="check-list">
                {splitLayers(activeRow["分层字段"]).map((layer) => (
                  <label key={layer} className="check-item">
                    <input
                      type="checkbox"
                      checked={selectedLayers.includes(layer)}
                      onChange={() => toggleLayer(layer)}
                    />
                    <span>{layer}</span>
                  </label>
                ))}
              </div>
              <div className="field-inline">
                <label>
                  <span>每层源行数</span>
                  <input value={sourceRows} placeholder="默认 5000" onChange={(e) => setSourceRows(e.target.value)} />
                </label>
                <label>
                  <span>模拟中断批次</span>
                  <input value={failAt} placeholder="留空则完整跑完" onChange={(e) => setFailAt(e.target.value)} />
                </label>
              </div>
              <button className="btn primary" type="button" disabled={selectedLayers.length === 0} onClick={() => void submitRun()}>
                整组提交运行
              </button>
              <div className="field-inline">
                <label>
                  <span>新样本比例</span>
                  <input value={newRatio} onChange={(e) => setNewRatio(e.target.value)} />
                </label>
                <button className="btn" type="button" onClick={() => void saveRatio()}>
                  更新比例（只影响新批次）
                </button>
              </div>
            </div>

            <div className="panel-block">
              <h4>版本历史（历史结果不覆盖）</h4>
              <table className="data-table">
                <thead>
                  <tr><th>运行</th><th>版本</th><th>比例</th><th>状态</th><th>完成位置</th><th></th></tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr key={run.id}>
                      <td>{run.运行编号}</td>
                      <td>v{run.版本}{run.结果已清理 ? '（结果已清理）' : ''}</td>
                      <td>{run.样本比例}</td>
                      <td><span className={batchTagClass(run.状态 === '已完成' ? '已完成' : run.状态 === '部分失败' ? '失败' : '待处理')}>{run.状态}</span></td>
                      <td>{run.完成位置} 批</td>
                      <td><button className="link" type="button" onClick={() => void showRun(run.id)}>查看断点</button></td>
                    </tr>
                  ))}
                  {runs.length === 0 && (
                    <tr><td className="empty-state" colSpan={6}>还没有采样运行，先整组提交一个批次组</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {currentRun && (
            <div className="panel-block">
              <h4>
                运行 {currentRun.运行编号}（v{currentRun.版本}）断点明细：{currentRun.完成位置} 批
                <span className={batchTagClass(currentRun.状态 === '已完成' ? '已完成' : currentRun.状态 === '部分失败' ? '失败' : '待处理')}>
                  {currentRun.状态}
                </span>
              </h4>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>批次</th><th>分层字段</th><th>层内批次</th><th>随机种子断点</th>
                    <th>样本区间</th><th>计划/已采样</th><th>写入次数</th><th>状态</th><th>失败原因</th>
                  </tr>
                </thead>
                <tbody>
                  {currentRun.批次.map((batch) => (
                    <tr key={batch.序号}>
                      <td>#{batch.序号}</td>
                      <td>{batch.分层字段}</td>
                      <td>{batch.层内批次}/{batch.层内批数}</td>
                      <td>{batch.随机种子断点}</td>
                      <td>{batch.样本起点}–{batch.样本终点}</td>
                      <td>{batch.计划行数}/{batch.已采样行数}</td>
                      <td>{batch.写入次数}</td>
                      <td><span className={batchTagClass(batch.状态)}>{batch.状态}</span></td>
                      <td>{batch.失败原因 ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="panel-actions">
                <button
                  className="btn primary"
                  type="button"
                  disabled={currentRun.状态 === '已完成'}
                  onClick={() => void resumeRun()}
                >
                  重试未完成批次（沿用随机种子断点）
                </button>
                <span className="page-desc">{currentRun.消息}</span>
              </div>
            </div>
          )}

          {output && (
            <div className="panel-block">
              <h4>输出表「{output.输出表名}」核对（共 {output.总行数} 行，保留最近 3 个版本）</h4>
              <table className="data-table">
                <thead>
                  <tr><th>版本</th><th>运行</th><th>样本比例</th><th>状态</th><th>行数</th><th>分层行数</th></tr>
                </thead>
                <tbody>
                  {output.版本.map((version) => (
                    <tr key={version.版本}>
                      <td>v{version.版本}{version.结果已清理 ? '（已清理）' : ''}</td>
                      <td>{version.运行编号}</td>
                      <td>{version.样本比例}</td>
                      <td>{version.状态}</td>
                      <td>{version.行数}</td>
                      <td>
                        {Object.entries(version.分层行数).map(([layer, count]) => `${layer}: ${count}`).join('；') || '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      <footer className="page-foot">
        <span>共 {total} 条数据采样管理记录</span>
        {notice && <span className="ok-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
