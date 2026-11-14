/** Durable portal conversations and their worker-ticket associations. */
import { family } from '../kv.ts'
import { key, latestAssignment, ticketOutcome, type Engine } from '../core/index.ts'
import { allowedSender } from '../policy.ts'
import { ticketCard } from './cards.ts'
import type { Store } from './store.ts'

export interface PortalTask { id: string; instruction: string; dependsOn: string[]; ticketId?: string }
export interface PortalConversation { id: string; createdAt: string; updatedAt: string; lastTriggerId: string; lastTriggerAt: string; tasks: PortalTask[] }
const conversations = family<PortalConversation, [id: string]>('booth-portal-conversation:')
const threads = family<string, [thread: string, actor: string]>('booth-portal-thread:')
const owners = family<{ conversationId: string; taskId: string }, [ticketId: string]>('booth-portal-ticket:')

/** The portal's associations share the orchestrator's transactional SQLite database. */
export class Portal {
  constructor(private readonly store: Pick<Store, 'get' | 'set' | 'atomic' | 'keys' | 'triggers' | 'tickets' | 'settings'>) {}
  get(id: string): PortalConversation | undefined { return this.store.get(conversations(id)) }
  owner(ticketId: string): { conversationId: string; taskId: string } | undefined { return this.store.get(owners(ticketId)) }
  /** Keep conversation ordering current when a worker sends a report or asks the user. */
  touch(ticketId: string): void {
    const owner = this.owner(ticketId)
    const conversation = owner && this.get(owner.conversationId)
    if (!conversation) return
    conversation.updatedAt = new Date().toISOString()
    this.store.set(conversations(conversation.id), conversation)
  }
  open(id: string): PortalConversation {
    if (!id.trim()) throw new Error('conversation id is required')
    return this.store.atomic(() => {
      const existing = this.get(id)
      if (existing) return existing
      const now = new Date().toISOString()
      const conversation: PortalConversation = { id, createdAt: now, updatedAt: now, lastTriggerId: id, lastTriggerAt: now, tasks: [] }
      this.store.set(conversations(id), conversation)
      return conversation
    })
  }
  /** Find the durable conversation for a console message, creating it for its first message. */
  forTrigger(triggerId: string): PortalConversation {
    return this.store.atomic(() => {
      const trigger = this.store.triggers.get(triggerId)
      const disposition = this.store.triggers.disposition(triggerId)
      if (!trigger || !['local', 'weixin', 'feishu'].includes(trigger.source) || !trigger.human || !disposition || disposition.filtered || !trigger.threads[0] || !allowedSender(this.store.settings(), trigger.actor)) {
        throw new Error('portal conversation needs an admitted local, WeChat or Feishu human message')
      }
      const thread = trigger.threads[0]
      if (thread.source !== trigger.source || trigger.meta.portalTask) throw new Error('not a portal conversation')
      const index = threads(thread.id, trigger.actor.id)
      const existing = this.store.get(index)
      if (existing) {
        const conversation = this.get(existing)
        if (!conversation) throw new Error('portal conversation index is inconsistent')
        let changed = false
        if (trigger.at >= conversation.lastTriggerAt) {
          conversation.lastTriggerId = triggerId
          conversation.lastTriggerAt = trigger.at
          changed = true
        }
        if (trigger.at > conversation.updatedAt) {
          conversation.updatedAt = trigger.at
          changed = true
        }
        if (changed) this.store.set(conversations(conversation.id), conversation)
        return conversation
      }
      const conversation = this.open(triggerId)
      conversation.lastTriggerId = triggerId
      conversation.lastTriggerAt = trigger.at
      if (trigger.at > conversation.updatedAt) conversation.updatedAt = trigger.at
      this.store.set(conversations(conversation.id), conversation)
      this.store.set(index, conversation.id)
      return conversation
    })
  }
  /** A paged, newest-activity-first view for the booth console and CLI. */
  list(offset = 0, limit = 20) {
    const conversationsPage = this.store.keys(conversations).flatMap(name => {
      const value = this.store.get(name as ReturnType<typeof conversations>)
      return value ? [value] : []
    }).sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
    const total = conversationsPage.length
    const items = conversationsPage.slice(offset, offset + limit).map(conversation => ({ ...conversation,
      tasks: conversation.tasks.map(task => {
        const ticket = task.ticketId ? this.store.tickets.get(task.ticketId) : undefined
        return { ...task, ...(ticket ? { ticket: ticketCard(ticket) } : {}) }
      }) }))
    return { items, next_offset: offset + items.length < total ? offset + items.length : null }
  }
  /** Dependencies must already exist; task ids make retried planning idempotent. */
  plan(conversationId: string, id: string, instruction: string, dependsOn: string[] = []): PortalTask {
    if (!id.trim() || !instruction.trim()) throw new Error('task id and instruction are required')
    if (new Set(dependsOn).size !== dependsOn.length) throw new Error('duplicate task dependency')
    return this.store.atomic(() => {
      const conversation = this.get(conversationId)
      if (!conversation) throw new Error('unknown conversation')
      const existing = conversation.tasks.find(task => task.id === id)
      if (existing) {
        if (existing.instruction !== instruction || JSON.stringify(existing.dependsOn) !== JSON.stringify(dependsOn)) throw new Error('task id already has a different plan')
        return existing
      }
      if (dependsOn.some(dep => !conversation.tasks.some(task => task.id === dep))) throw new Error('unknown task dependency')
      const task: PortalTask = { id, instruction, dependsOn: [...dependsOn] }
      conversation.tasks.push(task)
      conversation.updatedAt = new Date().toISOString()
      this.store.set(conversations(conversationId), conversation)
      return task
    })
  }
  /** Bind one worker ticket to one task, without stealing an existing association. */
  bind(conversationId: string, taskId: string, ticketId: string): PortalTask {
    if (!ticketId.trim()) throw new Error('ticket id is required')
    return this.store.atomic(() => {
      const conversation = this.get(conversationId)
      const task = conversation?.tasks.find(item => item.id === taskId)
      if (!conversation || !task) throw new Error('unknown task')
      const owner = this.owner(ticketId)
      if (owner && (owner.conversationId !== conversationId || owner.taskId !== taskId)) throw new Error('ticket belongs to another task')
      if (task.ticketId && task.ticketId !== ticketId) throw new Error('task already has a ticket')
      task.ticketId = ticketId
      conversation.updatedAt = new Date().toISOString()
      this.store.set(conversations(conversationId), conversation)
      this.store.set(owners(ticketId), { conversationId, taskId })
      return task
    })
  }
  /** Accept a planned task through the existing policy gate, once, and record its ticket atomically. */
  dispatch(conversationId: string, taskId: string, name: string, engine: Pick<Engine, 'receive' | 'decide'>): PortalTask {
    return this.store.atomic(() => {
      const conversation = this.get(conversationId)
      const task = conversation?.tasks.find(item => item.id === taskId)
      if (!conversation || !task) throw new Error('unknown task')
      if (task.ticketId) return task
      const origin = this.store.triggers.get(conversationId)
      const disposition = this.store.triggers.disposition(conversationId)
      if (!origin || !['local', 'weixin', 'feishu'].includes(origin.source) || !origin.human || !disposition || disposition.filtered || !allowedSender(this.store.settings(), origin.actor)) throw new Error('portal conversation needs an admitted local, WeChat or Feishu human message')
      for (const dependency of task.dependsOn) {
        const prerequisite = conversation.tasks.find(item => item.id === dependency)
        const ticket = prerequisite?.ticketId && this.store.tickets.get(prerequisite.ticketId)
        const outcome = ticket && ticketOutcome(ticket)
        const assignment = ticket ? latestAssignment(ticket) : undefined
        const report = assignment?.report
        const currentReady = report?.state === 'ready' && (!assignment?.deliveredAt || assignment.deliveredAt < report.at)
        if (!ticket || !(outcome === 'completed' || outcome === 'merged' || (!outcome && currentReady))) {
          throw new Error(`dependency ${dependency} has not completed`)
        }
      }
      const id = `portal-${key(conversationId, taskId)}`
      const thread = `portal-task-${key(conversationId, taskId)}`
      const trigger = { ...origin, id, threads: [{ source: origin.source, id: thread }], at: new Date().toISOString(),
        text: task.instruction, channel: undefined, meta: { ...origin.meta, portalConversation: conversationId, portalTask: taskId } }
      const received = engine.receive(trigger)
      if (received !== 'stored' && received !== 'duplicate') throw new Error('the task was rejected by the source')
      const decision = engine.decide(id, { kind: 'accept', title: task.instruction.slice(0, 200), scope: task.instruction,
        reason: { code: 'explicitRouted' }, name })
      if (decision.kind !== 'accept' || !decision.ticketId) throw new Error('the task was not accepted by policy')
      return this.bind(conversationId, taskId, decision.ticketId)
    })
  }

  /** Deliver a person's follow-up without subscribing the worker to the portal conversation. */
  forward(conversationId: string, taskId: string, triggerId: string, engine: Pick<Engine, 'receive' | 'attach'>): PortalTask {
    return this.store.atomic(() => {
      const conversation = this.forTrigger(triggerId)
      if (conversation.id !== conversationId) throw new Error('message belongs to another conversation')
      const task = conversation.tasks.find(item => item.id === taskId)
      if (!task?.ticketId) throw new Error('task has no worker')
      const original = this.store.triggers.get(triggerId)!
      const id = `portal-input-${key(conversationId, taskId, triggerId)}`
      const thread = `portal-input-${key(conversationId, taskId, triggerId)}`
      const trigger = { ...original, id, threads: [{ source: original.source, id: thread }], channel: undefined,
        meta: { ...original.meta, portalConversation: conversationId, portalTask: taskId, portalOriginalTrigger: triggerId } }
      const received = engine.receive(trigger)
      if (received === 'filtered') throw new Error('the message was rejected by the source')
      // A retry after the first attach must not ask the Engine to attach a message its worker already owns.
      const delivered = this.store.tickets.get(task.ticketId)?.inputs.some(input => input.kind === 'trigger' && input.triggerId === id)
      if (!delivered) engine.attach(task.ticketId, id)
      return task
    })
  }

}
