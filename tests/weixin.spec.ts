import { chmodSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { expect, it, vi } from 'vitest'
import { createIlinkClient, receiveIlinkBatch, weixinKind } from '../src/sources/weixin.ts'
import { KV } from '../src/kv.ts'

it('polls iLink with the durable cursor and only advances it after triggers are stored', async () => {
  const state = new Map<string, unknown>([[KV.weixin.cursor, 'before']])
  const received: Record<string, unknown>[] = []
  const context = { state: { get: (key: string) => state.get(key), set: (key: string, value: unknown) => state.set(key, value) },
    receive: async (trigger: Record<string, unknown>) => { received.push(trigger); return 'stored' } }
  const client = { updates: vi.fn(async (cursor: string) => {
    expect(cursor).toBe('before')
    return { ret: 0, get_updates_buf: 'after', msgs: [{ client_id: 'm-1', message_id: '123', from_user_id: 'user@im.wechat',
      to_user_id: 'bot@im.bot', message_type: 1, message_state: 2, context_token: 'ctx-1', create_time_ms: 1794490000000,
      item_list: [{ type: 1, text_item: { text: 'hello' } }] }] }
  }) }
  await receiveIlinkBatch(context as never, client as never)
  expect(received[0]).toMatchObject({ id: 'weixin-m-1', source: 'weixin', actor: { source: 'weixin', id: 'user@im.wechat' },
    human: true, addressed: 'direct', text: 'hello', channel: { source: 'weixin', thread: 'user@im.wechat', address: { contextToken: 'ctx-1' } } })
  expect(state.get(KV.weixin.cursor)).toBe('after')
})

it('keeps a valid text message when iLink uses a non-2 message state', async () => {
  const state = new Map<string, unknown>()
  const received: Record<string, unknown>[] = []
  const context = { state: { get: (key: string) => state.get(key), set: (key: string, value: unknown) => state.set(key, value) },
    receive: async (trigger: Record<string, unknown>) => { received.push(trigger); return 'stored' } }
  await receiveIlinkBatch(context as never, { updates: async () => ({ get_updates_buf: 'next', msgs: [{ client_id: 'state-1', from_user_id: 'wx-user',
    message_type: 1, message_state: 1, context_token: 'ctx', item_list: [{ type: 1, text_item: { text: 'still valid' } }] }] }) })
  expect(received).toHaveLength(1)
  expect(received[0]).toMatchObject({ text: 'still valid', actor: { id: 'wx-user' } })
})

it('registers a WeChat direct message as a durable Portal conversation', async () => {
  const { startRuntime } = await import('./fixtures/host.ts')
  const h = await startRuntime()
  try {
    h.runtime.sources.register(weixinKind(h.directory))
    const settings = h.runtime.store.settings()
    h.runtime.store.saveSettings({ ...settings,
      sources: { ...settings.sources, weixin: { enabled: true, config: {} } },
      policy: { ...settings.policy, sources: { ...settings.policy.sources, weixin: { allowed: [{ id: 'wx-user' }] } } },
    })
    const trigger = { id: 'weixin-message-1', source: 'weixin', threads: [{ source: 'weixin', id: 'wx-user' }],
      actor: { source: 'weixin', id: 'wx-user' }, human: true, addressed: 'direct' as const,
      at: new Date().toISOString(), text: '你好', channel: { source: 'weixin', thread: 'wx-user', address: { userId: 'wx-user', contextToken: 'ctx' } },
      meta: { kind: 'direct', direct: true } }
    expect(h.runtime.receive(trigger)).toBe('stored')
    await h.runtime.idle()
    const result = await h.runTool(h.runtime.masterId, 'booth_portal_conversation', { trigger_id: trigger.id })
    expect(result.id).toBe(trigger.id)
    expect(h.runtime.portal.forTrigger(trigger.id).lastTriggerId).toBe(trigger.id)
  } finally { await h.stop() }
})

it('syncs the QR-bound user and delivers a portal reply to that WeChat context', async () => {
  const { startRuntime } = await import('./fixtures/host.ts')
  const h = await startRuntime()
  const calls: { url: string; body: Record<string, unknown> }[] = []
  const fetch = vi.fn(async (input: URL | RequestInfo, init?: RequestInit) => {
    calls.push({ url: String(input), body: JSON.parse(String(init?.body)) })
    return new Response('{"ret":0}')
  })
  vi.stubGlobal('fetch', fetch)
  try {
    const credentials = join(h.directory, 'weixin-credentials.json')
    writeFileSync(credentials, JSON.stringify({ bot_token: 'test-token', ilink_user_id: 'wx-owner' }), { mode: 0o600 })
    chmodSync(credentials, 0o600)
    const kind = weixinKind(h.directory)
    h.runtime.sources.register(kind)
    const synced = await kind.policy!.sync!.run(h.runtime, { allowed: [] }, new AbortController().signal)
    expect(synced.allowed).toEqual([{ id: 'wx-owner' }])

    const settings = h.runtime.store.settings()
    h.runtime.store.saveSettings({ ...settings, sources: { ...settings.sources, weixin: { enabled: true, config: {} } },
      policy: { ...settings.policy, sources: { ...settings.policy.sources, weixin: synced } } })
    const state = new Map<string, unknown>()
    const context = { state: { get: (key: string) => state.get(key), set: (key: string, value: unknown) => state.set(key, value) },
      receive: (trigger: unknown) => h.runtime.receive(trigger as never) }
    await receiveIlinkBatch(context as never, { updates: async () => ({ get_updates_buf: 'next', msgs: [{ client_id: 'm-portal', from_user_id: 'wx-owner',
      message_type: 1, message_state: 2, context_token: 'reply-context', item_list: [{ type: 1, text_item: { text: '你好' } }] }] }) })
    await h.runtime.idle()
    const triggerId = 'weixin-m-portal'
    const conversation = await h.runTool(h.runtime.masterId, 'booth_portal_conversation', { trigger_id: triggerId })
    await h.runTool(h.runtime.masterId, 'orchestrator_route', { decisions: [{ kind: 'ignore', trigger_id: triggerId, reason: 'handled by portal' }] })
    const reply = await h.runTool(h.runtime.masterId, 'booth_portal_reply', { conversation_id: conversation.id, text: '你好，我是团队负责人。' })
    expect(reply.status).toBe('sent')
    expect(calls).toHaveLength(1)
    expect(calls[0]!.url).toContain('/ilink/bot/sendmessage')
    expect(calls[0]!.body.msg).toMatchObject({ to_user_id: 'wx-owner', context_token: 'reply-context',
      item_list: [{ type: 1, text_item: { text: '你好，我是团队负责人。' } }] })
  } finally { vi.unstubAllGlobals(); await h.stop() }
})

it('does not advance the iLink cursor when storing a received message fails', async () => {
  const state = new Map<string, unknown>([[KV.weixin.cursor, 'before']])
  const context = { state: { get: (key: string) => state.get(key), set: (key: string, value: unknown) => state.set(key, value) },
    receive: async () => { throw new Error('storage unavailable') } }
  const client = { updates: async () => ({ ret: 0, get_updates_buf: 'after', msgs: [{ client_id: 'm-1', from_user_id: 'u', message_type: 1,
    message_state: 2, context_token: 'ctx', item_list: [{ type: 1, text_item: { text: 'hello' } }] }] }) }
  await expect(receiveIlinkBatch(context as never, client as never)).rejects.toThrow('storage unavailable')
  expect(state.get(KV.weixin.cursor)).toBe('before')
})

it('sends a text reply using the saved user context and stable message id', async () => {
  const calls: { url: string; body: Record<string, unknown> }[] = []
  const client = createIlinkClient({ token: 'secret', fetch: (async (input, init) => {
    calls.push({ url: String(input), body: JSON.parse(String(init?.body)) })
    return new Response('{"ret":0}')
  }) as typeof fetch })
  await client.send({ userId: 'user@im.wechat', contextToken: 'ctx', messageId: 'out-1', text: 'reply' })
  expect(calls[0]!.url).toContain('/ilink/bot/sendmessage')
  expect(calls[0]!.body.msg).toMatchObject({ client_id: 'out-1', to_user_id: 'user@im.wechat', context_token: 'ctx',
    item_list: [{ type: 1, text_item: { text: 'reply' } }] })
})
