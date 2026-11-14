import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it } from 'vitest'
import { Portal } from '../src/app/portal.ts'
import { Store } from '../src/app/store.ts'

it('persists task ownership across restarts and rejects conflicting retries', () => {
  const dir = mkdtempSync(join(tmpdir(), 'portal-'))
  try {
    const path = join(dir, 'state.sqlite')
    let store = new Store(path)
    let portal = new Portal(store)
    portal.open('conversation')
    portal.open('another-conversation')
    expect(portal.list(0, 1).next_offset).toBe(1)
    portal.plan('conversation', 'a', 'inspect')
    portal.plan('conversation', 'b', 'implement', ['a'])
    portal.bind('conversation', 'a', 'ticket-1')
    store.close()
    store = new Store(path)
    portal = new Portal(store)
    expect(portal.open('conversation').tasks).toHaveLength(2)
    expect(portal.plan('conversation', 'a', 'inspect').ticketId).toBe('ticket-1')
    expect(portal.bind('conversation', 'a', 'ticket-1').ticketId).toBe('ticket-1')
    expect(portal.owner('ticket-1')).toEqual({ conversationId: 'conversation', taskId: 'a' })
    expect(() => portal.bind('conversation', 'b', 'ticket-1')).toThrow('another task')
    expect(() => portal.plan('conversation', 'a', 'different')).toThrow('different plan')
    expect(() => portal.plan('conversation', 'c', 'work', ['missing'])).toThrow('unknown task dependency')
    expect(portal.get('conversation')?.tasks).toHaveLength(2)
    expect(portal.list(1, 1).items).toHaveLength(1)
    store.close()
  } finally { rmSync(dir, { recursive: true, force: true }) }
})

it('dispatches distinct tasks once and waits for a prerequisite worker', async () => {
  const { z } = await import('zod')
  const { startRuntime } = await import('./fixtures/host.ts')
  const h = await startRuntime()
  try {
    const label = { en: 'Console', zh: '控制台' }
    h.runtime.sources.register({ kind: 'local', title: label, description: label, schema: z.object({}), fields: [], health: [],
      scope: () => ({ listened: true, observed: false, row: 'console' }),
      channel: { send: async () => 'sent', link: () => ({ url: '', label }) },
      policy: { schema: z.object({ allowed: z.array(z.object({ id: z.string() })).default([]) }), fields: [], explicit: label,
        rows: () => [{ id: 'console', label, note: label, cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false } }] } })
    const settings = h.runtime.store.settings()
    h.runtime.store.saveSettings({ ...settings, sources: { ...settings.sources, local: { enabled: true, config: {} } },
      policy: { ...settings.policy, sources: { ...settings.policy.sources, local: { allowed: [{ id: 'operator' }] } } } })
    const id = 'local-request', thread = 'console-conversation'
    expect(h.runtime.receive({ id, source: 'local', threads: [{ source: 'local', id: thread }], actor: { source: 'local', id: 'operator' },
      human: true, addressed: 'direct', at: new Date().toISOString(), text: 'Investigate and implement', meta: {},
      channel: { source: 'local', thread, address: {} } })).toBe('stored')
    await h.runtime.idle()
    const run = (name: string, args: unknown) => h.runTool(h.runtime.masterId, name, args)
    const initial = await run('booth_portal_conversation', { trigger_id: id })
    expect(await h.api('GET', 'portal')).toMatchObject({ status: 200, body: { items: [expect.objectContaining({ id })], next_offset: null } })
    expect(initial.id).toBe(id)
    expect((await run('booth_portal_reply', { conversation_id: id, text: 'I will coordinate the work.' })).status).toBe('sent')
    const outbox = () => h.runtime.store.db.prepare('SELECT value FROM outbox ORDER BY rowid').all()
      .map(row => JSON.parse(String(row.value)) as { id: string; text: string })
    const firstReply = outbox().at(-1)!
    const followup = { id: 'local-followup', source: 'local', threads: [{ source: 'local', id: thread }], actor: { source: 'local', id: 'operator' },
      human: true, addressed: 'direct' as const, at: new Date().toISOString(), text: 'Please proceed', meta: {}, channel: { source: 'local', thread, address: {} } }
    expect(h.runtime.receive(followup)).toBe('stored')
    expect((await run('booth_portal_conversation', { trigger_id: followup.id })).id).toBe(id)
    expect((await run('booth_portal_reply', { conversation_id: id, text: '收到。' })).status).toBe('sent')
    const replyLines = outbox()
    await run('booth_portal_reply', { conversation_id: id, text: '收到。' })
    expect(replyLines.at(-1)!.text).toBe('收到。')
    expect(replyLines.at(-1)!.id).not.toBe(firstReply.id)
    expect(outbox()).toHaveLength(replyLines.length), 'the exact same reply in one turn is deduplicated'
    await run('booth_portal_plan', { conversation_id: id, task_id: 'research', instruction: 'Inspect the project', depends_on: [] })
    await run('booth_portal_plan', { conversation_id: id, task_id: 'implement', instruction: 'Implement the change', depends_on: ['research'] })
    await run('booth_portal_plan', { conversation_id: id, task_id: 'docs', instruction: 'Review the documentation', depends_on: [] })
    await expect(run('booth_portal_dispatch', { conversation_id: id, task_id: 'implement', worktree_name: 'implement-change' })).rejects.toThrow('dependency research')
    const first = await run('booth_portal_dispatch', { conversation_id: id, task_id: 'research', worktree_name: 'inspect-project' })
    const second = await run('booth_portal_dispatch', { conversation_id: id, task_id: 'research', worktree_name: 'inspect-project' })
    expect(second.task).toEqual(first.task)
    const ticketId = (first.task as { ticketId: string }).ticketId
    const parallel = await run('booth_portal_dispatch', { conversation_id: id, task_id: 'docs', worktree_name: 'review-docs' })
    expect((parallel.task as { ticketId: string }).ticketId).not.toBe(ticketId)
    expect(h.runtime.portal.owner(ticketId)).toEqual({ conversationId: id, taskId: 'research' })
    expect(await h.api('GET', 'portal')).toMatchObject({ body: { items: [expect.objectContaining({
      tasks: expect.arrayContaining([expect.objectContaining({ id: 'research', ticket: expect.objectContaining({ ticket_id: ticketId }) })]),
    })] } })
    await h.runtime.idle()
    await run('booth_portal_forward', { conversation_id: id, task_id: 'research', trigger_id: followup.id })
    await run('booth_portal_forward', { conversation_id: id, task_id: 'research', trigger_id: followup.id })
    const ticket = h.runtime.store.tickets.get(ticketId)!
    const forwarded = ticket.inputs.filter(input => input.kind === 'trigger' && input.triggerId !== ticket.origin.triggerId)
    expect(forwarded).toHaveLength(1)
    expect(h.runtime.store.triggers.get(forwarded[0]!.kind === 'trigger' ? forwarded[0]!.triggerId : '')?.text).toBe('Please proceed')
    expect(h.runtime.store.threads.get(`local:${thread}`)?.subscriptions ?? []).not.toContain(ticketId)
    const ack = await h.runTool(`worker-${ticketId}`, 'orchestrator_ack', { trigger_id: ticket.origin.triggerId, text: 'Inspecting.' })
    expect(ack.acknowledgement).toBe('portal')
    const beforeReport = h.runtime.portal.get(id)!.updatedAt
    await h.runTool(`worker-${ticketId}`, 'orchestrator_report', { state: 'ready', summary: 'Inspection completed.' })
    await h.runtime.idle()
    expect(h.runtime.portal.get(id)!.updatedAt >= beforeReport).toBe(true)
    expect(h.relays(h.runtime.masterId)).toContainEqual(expect.objectContaining({ kind: 'worker_report', portal: { conversationId: id, taskId: 'research' } }))
    const implemented = await run('booth_portal_dispatch', { conversation_id: id, task_id: 'implement', worktree_name: 'implement-change' })
    expect((implemented.task as { ticketId: string }).ticketId).not.toBe(ticketId)
    const secondTicket = (implemented.task as { ticketId: string }).ticketId
    const stop = { ...followup, id: 'local-stop', text: 'Stop the implementation' }
    expect(h.runtime.receive(stop)).toBe('stored')
    const stopped = await run('orchestrator_route', { decisions: [{ kind: 'stop', trigger_id: stop.id, ticket_id: secondTicket, reason: 'User requested stop' }] })
    expect((stopped.results as { decision: string }[])[0]?.decision).toBe('stop')
    expect(h.runtime.store.tickets.get(secondTicket)?.ended?.by).toBe('human')
  } finally { await h.stop() }
})

it('connects the real console Source to the portal and returns its reply to the outbox', async () => {
  const { writeFileSync, readFileSync, existsSync } = await import('node:fs')
  const { startRuntime } = await import('./fixtures/host.ts')
  const { waitUntil } = await import('./fixtures/async.ts')
  // The console Source is a separate JavaScript bundle beside this submodule.
  // @ts-expect-error The bundle intentionally has no TypeScript declarations.
  const { apply } = await import('../../local-source/index.js')
  const h = await startRuntime()
  const cleanups: (() => void)[] = []
  try {
    const settings = h.runtime.store.settings()
    h.runtime.store.saveSettings({ ...settings, sources: { ...settings.sources, local: { enabled: true, config: {} } },
      policy: { ...settings.policy, sources: { ...settings.policy.sources, local: { allowed: [{ id: 'operator' }] } } } })
    writeFileSync(join(h.directory, 'booth-inbox.jsonl'), JSON.stringify({ id: 'first', text: '你好', thread: 'portal-one', sender: 'operator' }) + '\n')
    apply({ get: (name: string) => name === 'orchestrator' ? h.runtime : undefined,
      effect: (fn: () => () => void) => { cleanups.push(fn()) } }, { stateDir: h.directory, pollMs: 5 })
    await waitUntil(() => !!h.runtime.store.triggers.get('local-first'))
    await h.runtime.idle()
    const run = (name: string, args: unknown) => h.runTool(h.runtime.masterId, name, args)
    const conversation = await run('booth_portal_conversation', { trigger_id: 'local-first' })
    expect(conversation.id).toBe('local-first')
    await run('orchestrator_route', { decisions: [{ kind: 'ignore', trigger_id: 'local-first', reason: 'handled by portal' }] })
    expect((await run('booth_portal_reply', { conversation_id: 'local-first', text: '你好，我是团队负责人。' })).status).toBe('sent')
    const outbox = join(h.directory, 'booth-outbox.jsonl')
    expect(existsSync(outbox)).toBe(true)
    const reply = JSON.parse(readFileSync(outbox, 'utf8').trim()) as { text: string; thread: string }
    expect(reply).toMatchObject({ text: '你好，我是团队负责人。', thread: 'portal-one' })
  } finally {
    for (const cleanup of cleanups) cleanup()
    await h.stop()
  }
})

it('accepts an admitted Feishu message as a portal conversation', async () => {
  const { startRuntime, feishu } = await import('./fixtures/host.ts')
  const h = await startRuntime()
  try {
    const trigger = feishu('9', '@dsh-booth 你好')
    expect(h.runtime.receive(trigger)).toBe('stored')
    await h.runtime.idle()
    const conversation = await h.runTool(h.runtime.masterId, 'booth_portal_conversation', { trigger_id: trigger.id })
    expect(conversation.id).toBe(trigger.id)
    expect((await h.runTool(h.runtime.masterId, 'booth_portal_reply', { conversation_id: trigger.id, text: '你好，我是团队负责人。' })).status).toBe('sent')
  } finally { await h.stop() }
})
