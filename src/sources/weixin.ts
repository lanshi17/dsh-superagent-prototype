/** WeChat iLink direct-message Source. The cursor advances only after every inbound Trigger is durable. */
import { randomBytes } from 'node:crypto'
import { readFileSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { z } from 'zod'
import { KV } from '../kv.ts'
import type { Trigger } from '../core/index.ts'
import type { SourceContext } from './host.ts'
import type { Localized, SourceKind } from './kind.ts'

export const SOURCE = 'weixin'
const CredentialsSchema = z.object({ bot_token: z.string().min(1), ilink_bot_id: z.string().optional(), ilink_user_id: z.string().optional(), baseurl: z.string().url().optional() }).passthrough()
const ConfigSchema = z.object({ pollMs: z.number().int().min(100).max(60000).default(1000) }).strict()
type Config = z.infer<typeof ConfigSchema>
const textItem = z.object({ type: z.literal(1), text_item: z.object({ text: z.string() }).passthrough() }).passthrough()
const InboundSchema = z.object({ client_id: z.string().optional(), message_id: z.union([z.string(), z.number()]).optional(),
  from_user_id: z.string().min(1), message_type: z.number(), message_state: z.number().optional(), context_token: z.string().optional(),
  create_time_ms: z.number().optional(), item_list: z.array(z.unknown()).default([]) }).passthrough()
const UpdatesSchema = z.object({ ret: z.number().optional(), errcode: z.number().optional(), errmsg: z.string().optional(),
  msgs: z.array(z.unknown()).default([]), get_updates_buf: z.string().optional() }).passthrough()

export interface IlinkMessage { client_id?: string; message_id?: string | number; from_user_id: string; message_type: number; message_state?: number;
  context_token?: string; create_time_ms?: number; item_list: unknown[] }
export interface IlinkClient {
  updates(cursor: string, signal?: AbortSignal): Promise<{ msgs: unknown[]; get_updates_buf?: string }>
  send(message: { userId: string; contextToken: string; messageId: string; text: string }, signal?: AbortSignal): Promise<void>
}
/** Small iLink text client matching zhishi's endpoint and header configuration. */
export function createIlinkClient(options: { token: string; baseUrl?: string; fetch?: typeof fetch }): IlinkClient {
  const base = new URL(options.baseUrl ?? 'https://ilinkai.weixin.qq.com')
  if (base.protocol !== 'https:' && base.hostname !== 'localhost' && base.hostname !== '127.0.0.1') throw new Error('iLink API must use HTTPS')
  const request = options.fetch ?? globalThis.fetch
  const baseInfo = { channel_version: '1.0.2' }
  const post = async (path: string, body: unknown, signal?: AbortSignal) => {
    const uin = Buffer.from(String(randomBytes(4).readUInt32BE(0))).toString('base64')
    const response = await request(new URL(path, base), { method: 'POST', signal,
      headers: { 'Content-Type': 'application/json', AuthorizationType: 'ilink_bot_token', 'X-WECHAT-UIN': uin, Authorization: `Bearer ${options.token}` },
      body: JSON.stringify(body) })
    if (!response.ok) throw new Error(`iLink ${path}: HTTP ${response.status}`)
    const value: unknown = await response.json()
    if (!value || typeof value !== 'object') throw new Error(`iLink ${path}: invalid response`)
    const result = value as { ret?: unknown; errcode?: unknown }
    if (typeof result.ret === 'number' && result.ret !== 0) throw new Error(`iLink ${path}: ret=${result.ret}`)
    if (typeof result.errcode === 'number' && result.errcode !== 0) throw new Error(`iLink ${path}: errcode=${result.errcode}`)
    return value as Record<string, unknown>
  }
  return {
    async updates(cursor, signal) {
      const value = UpdatesSchema.parse(await post('ilink/bot/getupdates', { get_updates_buf: cursor, base_info: baseInfo }, signal))
      return { msgs: value.msgs, ...(value.get_updates_buf ? { get_updates_buf: value.get_updates_buf } : {}) }
    },
    async send(message, signal) {
      if (!message.contextToken) throw new Error('iLink reply has no conversation context')
      await post('ilink/bot/sendmessage', { msg: { from_user_id: '', to_user_id: message.userId, client_id: message.messageId,
        message_type: 2, message_state: 2, context_token: message.contextToken,
        item_list: [{ type: 1, text_item: { text: message.text } }] }, base_info: baseInfo }, signal)
    },
  }
}

/** Store each trigger before committing the corresponding upstream cursor. */
export async function receiveIlinkBatch(ctx: Pick<SourceContext, 'state' | 'receive'>, client: Pick<IlinkClient, 'updates'>,
  signal?: AbortSignal): Promise<number> {
  const before = ctx.state.get(KV.weixin.cursor) ?? ''
  const result = await client.updates(before, signal)
  let received = 0
  for (const raw of result.msgs) {
    const parsed = InboundSchema.safeParse(raw)
    if (!parsed.success) continue
    const message = parsed.data
    // iLink can vary message_state across deliveries; it is not a reliable indication that an
    // otherwise valid inbound user message should be discarded. Match zhishi's gateway and use
    // message_type plus the reply context to identify messages this Source can process.
    if (message.message_type !== 1 || !message.context_token) continue
    const text = message.item_list.flatMap(item => {
      const parsedItem = textItem.safeParse(item)
      return parsedItem.success && parsedItem.data.text_item.text ? [parsedItem.data.text_item.text] : []
    }).join('')
    if (!text.trim()) continue
    const idPart = message.client_id || (message.message_id === undefined ? '' : String(message.message_id))
    if (!idPart) continue
    const trigger: Trigger = { id: `weixin-${idPart}`, source: SOURCE, threads: [{ source: SOURCE, id: message.from_user_id }],
      actor: { source: SOURCE, id: message.from_user_id }, human: true, addressed: 'direct',
      at: new Date(message.create_time_ms ?? Date.now()).toISOString(), text,
      channel: { source: SOURCE, thread: message.from_user_id, address: { userId: message.from_user_id, contextToken: message.context_token } },
      meta: { kind: 'direct', direct: true, messageId: String(message.message_id ?? idPart), clientId: idPart } }
    const outcome = await ctx.receive(trigger)
    if (outcome === 'filtered') continue
    received++
  }
  if (result.get_updates_buf) ctx.state.set(KV.weixin.cursor, result.get_updates_buf)
  return received
}

export function weixinKind(stateDir: string): SourceKind<Config> {
  const outbound = async (message: { userId: string; contextToken: string; messageId: string; text: string }) => {
    const credentials = credentialsOf(join(stateDir, 'weixin-credentials.json'))
    if (!credentials) throw new Error('WeChat iLink is not connected; run `booth wechat login`')
    await createIlinkClient({ token: credentials.bot_token, baseUrl: credentials.baseurl }).send(message)
  }
  return {
  kind: SOURCE, title: { zh: '微信 iLink', en: 'WeChat iLink' },
  description: { zh: '通过微信 iLink Bot 接收私聊并在原会话中回复。', en: 'Receive WeChat direct messages through iLink Bot and reply in the same conversation.' },
  schema: ConfigSchema, fields: [], health: ['weixin'],
  scope: ({ meta }) => meta.direct === true ? { listened: true, observed: false, row: 'direct' } : { listened: false, reason: 'direct_only' },
  channel: {
    allowed: address => !!address.userId && !!address.contextToken,
    async send(host, message) {
      const userId = message.channel.address.userId
      const contextToken = message.channel.address.contextToken
      if (!userId || !contextToken) return 'gone'
      await outbound({ userId, contextToken, messageId: message.id, text: message.text })
      return 'sent'
    },
    link: () => ({ url: '', label: { zh: '微信私聊', en: 'WeChat direct message' } }),
  },
  policy: {
    schema: z.object({ allowed: z.array(z.object({ id: z.string().trim().min(1).max(200), name: z.string().trim().max(100).optional() }).strict()).default([]) }).strict(),
    fields: [{ key: 'allowed', type: 'rows', label: { zh: '能通过微信发起任务的用户', en: 'WeChat users allowed to start work' },
      columns: [{ key: 'id', type: 'text', label: { zh: '微信用户 ID', en: 'WeChat user ID' } }] }],
    sync: {
      label: { zh: '同步扫码绑定的微信账号', en: 'Sync the WeChat account used to bind this bot' },
      ready: part => {
        try {
          const owner = credentialsOf(join(stateDir, 'weixin-credentials.json'))?.ilink_user_id
          return !!owner && !part.allowed.some(entry => entry.id === owner)
        } catch { return false }
      },
      async run(_host, part) {
        const owner = credentialsOf(join(stateDir, 'weixin-credentials.json'))?.ilink_user_id
        return owner && !part.allowed.some(entry => entry.id === owner) ? { ...part, allowed: [...part.allowed, { id: owner }] } : part
      },
    },
    explicit: { zh: '名单中的微信用户向 Bot 发送的私聊消息。', en: 'A direct message from a listed WeChat user.' },
    rows: () => [{ id: 'direct', label: { zh: '私聊', en: 'Direct messages' }, note: { zh: '接收', en: 'Received' },
      cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false } }],
  },
  }
}

function credentialsOf(path: string) {
  try {
    const mode = statSync(path).mode & 0o777
    if (mode & 0o077) throw new Error('iLink credentials file must be private (mode 0600)')
    return CredentialsSchema.parse(JSON.parse(readFileSync(path, 'utf8')))
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw error
  }
}

const TICK = (ms: number, signal: AbortSignal) => delay(ms, undefined, { signal }).catch(() => {})
export async function runWeixin(ctx: SourceContext, stateDir: string, signal: AbortSignal): Promise<void> {
  await ctx.ready
  let failures = 0
  while (!signal.aborted) {
    const config = ctx.config<Config>(SOURCE)
    if (!config) { ctx.report('weixin', { state: 'paused', detail: 'Off' }); await TICK(1000, signal); continue }
    let client: IlinkClient
    try {
      const credentials = credentialsOf(join(stateDir, 'weixin-credentials.json'))
      if (!credentials) throw new Error('WeChat iLink is not connected; run `booth wechat login`')
      client = createIlinkClient({ token: credentials.bot_token, baseUrl: credentials.baseurl })
    }
    catch (error) { ctx.report('weixin', { state: 'paused', detail: error instanceof Error ? error.message : String(error) }); await TICK(5000, signal); continue }
    const controller = new AbortController()
    const abort = () => controller.abort(signal.reason)
    signal.addEventListener('abort', abort, { once: true })
    const disable = setInterval(() => { if (!ctx.config<Config>(SOURCE)) controller.abort() }, 1000)
    try {
      const count = await receiveIlinkBatch(ctx, client, controller.signal)
      failures = 0
      if (!controller.signal.aborted) ctx.report('weixin', { state: 'ready', detail: count ? `Received ${count} WeChat message(s)` : 'Waiting for a WeChat message', at: new Date().toISOString() })
    } catch (error) {
      if (!controller.signal.aborted && !signal.aborted) {
        failures++
        ctx.report('weixin', { state: 'error', detail: `iLink poll failed (${failures}): ${error instanceof Error ? error.message : String(error)}` })
        await TICK(config.pollMs, signal)
      }
    } finally { clearInterval(disable); signal.removeEventListener('abort', abort) }
  }
}

