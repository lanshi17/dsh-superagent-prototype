/** Cordis lifecycle wrapper for the WeChat iLink Source. */
import type { Context } from '@deepseek-ai/cordis'
import { z } from 'zod'
import { sourceContext } from './host.ts'
import { runWeixin, weixinKind } from './weixin.ts'

export const name = 'dsh-superagent-weixin'
export const inject = ['orchestrator']
const Config = z.object({ stateDir: z.string().min(1) }).strict()

export function apply(ctx: Context, config: unknown): void {
  const { stateDir } = Config.parse(config)
  const kind = weixinKind(stateDir)
  ctx.effect(() => ctx.orchestrator.sources.register(kind), 'weixin: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, kind)
    const controller = new AbortController()
    const running = runWeixin(source, stateDir, controller.signal)
      .catch(error => { if (!controller.signal.aborted) source.report('weixin', { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  }, 'weixin: iLink long poll')
}
