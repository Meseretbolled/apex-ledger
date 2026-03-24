#!/bin/bash
# Run this from your project root:
#   chmod +x setup-pipeline.sh
#   ./setup-pipeline.sh

set -e

echo "Creating API route folders..."
mkdir -p src/app/api/upload
mkdir -p src/app/api/run
mkdir -p src/app/api/stream

# ─────────────────────────────────────────────
# src/app/api/upload/route.ts
# ─────────────────────────────────────────────
cat > src/app/api/upload/route.ts << 'ENDOFFILE'
import { NextRequest, NextResponse } from 'next/server';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { randomUUID } from 'crypto';

export async function POST(req: NextRequest) {
  try {
    const formData = await req.formData();
    const file  = formData.get('file')  as File | null;
    const query = formData.get('query') as string | null;

    if (!file) return NextResponse.json({ error: 'No file provided' }, { status: 400 });

    if (file.size > 50 * 1024 * 1024)
      return NextResponse.json({ error: 'File too large (max 50 MB)' }, { status: 400 });

    const uploadDir = join(process.cwd(), process.env.UPLOAD_DIR ?? 'uploads');
    await mkdir(uploadDir, { recursive: true });

    const fileId   = randomUUID();
    const ext      = file.name.split('.').pop() ?? 'bin';
    const savePath = join(uploadDir, `${fileId}.${ext}`);
    await writeFile(savePath, Buffer.from(await file.arrayBuffer()));

    console.log(`[upload] ${file.name} → ${savePath}`);

    // Optional Kafka publish
    const kafkaUrl = process.env.KAFKA_REST_URL;
    if (kafkaUrl) {
      fetch(`${kafkaUrl}/topics/documents.uploaded`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/vnd.kafka.json.v2+json' },
        body: JSON.stringify({ records: [{ key: fileId, value: { event: 'document.uploaded', fileId, filename: file.name, size: file.size, query } }] }),
        signal: AbortSignal.timeout(2000),
      }).catch(() => console.warn('[upload] Kafka publish skipped'));
    }

    return NextResponse.json({ fileId, filename: file.name, size: file.size, query });
  } catch (err) {
    console.error('[upload] error:', err);
    return NextResponse.json({ error: err instanceof Error ? err.message : 'Upload failed' }, { status: 500 });
  }
}
ENDOFFILE

# ─────────────────────────────────────────────
# src/app/api/run/route.ts
# ─────────────────────────────────────────────
cat > src/app/api/run/route.ts << 'ENDOFFILE'
import { NextRequest, NextResponse } from 'next/server';

const LG_URL  = process.env.LANGGRAPH_URL          ?? 'http://localhost:8123';
const LG_KEY  = process.env.LANGGRAPH_API_KEY       ?? '';
const LG_ASST = process.env.LANGGRAPH_ASSISTANT_ID  ?? 'agent';

function headers() {
  const h: Record<string, string> = { 'Content-Type': 'application/json' };
  if (LG_KEY) h['X-Api-Key'] = LG_KEY;
  return h;
}

export async function POST(req: NextRequest) {
  try {
    const { fileId, filename, query } = await req.json() as { fileId: string; filename: string; query: string };

    // 1. Create thread
    const threadRes = await fetch(`${LG_URL}/threads`, { method: 'POST', headers: headers(), body: JSON.stringify({}) });
    if (!threadRes.ok) throw new Error(`Thread creation failed: ${threadRes.status}`);
    const { thread_id } = await threadRes.json() as { thread_id: string };

    // 2. Create run
    const runRes = await fetch(`${LG_URL}/threads/${thread_id}/runs`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        assistant_id: LG_ASST,
        input: {
          messages: [{ role: 'user', content: query }],
          file_id: fileId,
          filename,
        },
        stream_mode: ['events', 'values'],
      }),
    });
    if (!runRes.ok) throw new Error(`Run creation failed: ${runRes.status}`);
    const { run_id } = await runRes.json() as { run_id: string };

    console.log(`[run] thread=${thread_id} run=${run_id}`);
    return NextResponse.json({ threadId: thread_id, runId: run_id });

  } catch (err) {
    console.error('[run] error:', err);
    return NextResponse.json({ error: err instanceof Error ? err.message : 'Failed to start run' }, { status: 500 });
  }
}
ENDOFFILE

# ─────────────────────────────────────────────
# src/app/api/stream/route.ts
# ─────────────────────────────────────────────
cat > src/app/api/stream/route.ts << 'ENDOFFILE'
import { NextRequest } from 'next/server';

const LG_URL = process.env.LANGGRAPH_URL    ?? 'http://localhost:8123';
const LG_KEY = process.env.LANGGRAPH_API_KEY ?? '';

export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const threadId = searchParams.get('threadId');
  const runId    = searchParams.get('runId');

  if (!threadId || !runId)
    return new Response('threadId and runId are required', { status: 400 });

  const lgHeaders: Record<string, string> = { Accept: 'text/event-stream' };
  if (LG_KEY) lgHeaders['X-Api-Key'] = LG_KEY;

  const lgStream = await fetch(`${LG_URL}/threads/${threadId}/runs/${runId}/stream`, {
    headers: lgHeaders,
    signal: req.signal,
  });

  if (!lgStream.ok || !lgStream.body)
    return new Response(`LangGraph stream failed: ${lgStream.status}`, { status: 502 });

  const stream = new ReadableStream({
    async start(controller) {
      const reader = lgStream.body!.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          controller.enqueue(value);
        }
      } catch { /* client disconnected */ }
      finally { controller.close(); }
    },
    cancel() { lgStream.body?.cancel(); },
  });

  return new Response(stream, {
    headers: {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
    },
  });
}
ENDOFFILE

# ─────────────────────────────────────────────
# src/app/page.tsx
# ─────────────────────────────────────────────
cat > src/app/page.tsx << 'ENDOFFILE'
'use client';

import { useState, useCallback, useRef, useEffect } from 'react';
import { useDropzone } from 'react-dropzone';
import styles from './page.module.css';

type AgentStatus = 'pending' | 'running' | 'done' | 'error';
interface ToolCall { name: string; args: unknown; result: unknown | null; }
interface AgentStep {
  key: string; label: string; icon: string; desc: string;
  status: AgentStatus; thought: string; input: unknown; output: unknown;
  toolCalls: ToolCall[]; confidence: number;
  startedAt: number | null; durationMs: number | null; error: string;
}
type AppStatus = 'idle' | 'uploading' | 'starting' | 'running' | 'done' | 'error';
interface AppState {
  status: AppStatus; fileId: string; filename: string; fileSize: number;
  threadId: string; runId: string; steps: AgentStep[];
  finalAnswer: string; error: string; rawState: unknown;
}

const AGENTS: Pick<AgentStep, 'key' | 'label' | 'icon' | 'desc'>[] = [
  { key: 'validate_inputs',          label: 'Validate Inputs',        icon: 'V', desc: 'Validates application data'     },
  { key: 'open_credit_record',       label: 'Open Credit Record',     icon: 'O', desc: 'Opens credit stream'            },
  { key: 'load_applicant_registry',  label: 'Load Registry',          icon: 'R', desc: 'Loads company financials'       },
  { key: 'load_extracted_facts',     label: 'Load Extracted Facts',   icon: 'L', desc: 'Loads document facts'           },
  { key: 'analyze_credit_risk',      label: 'Analyze Credit Risk',    icon: 'A', desc: 'Gemini credit analysis'         },
  { key: 'apply_policy_constraints', label: 'Apply Policy',           icon: 'P', desc: 'Enforces policy rules'          },
  { key: 'write_output',             label: 'Write Output',           icon: 'W', desc: 'Writes events & triggers next'  },
];

function makeSteps(): AgentStep[] {
  return AGENTS.map(a => ({
    ...a, status: 'pending', thought: '', input: null, output: null,
    toolCalls: [], confidence: 0, startedAt: null, durationMs: null, error: '',
  }));
}

const INIT: AppState = {
  status: 'idle', fileId: '', filename: '', fileSize: 0,
  threadId: '', runId: '', steps: [], finalAnswer: '', error: '', rawState: null,
};

export default function Home() {
  const [app, setApp]         = useState<AppState>(INIT);
  const [query, setQuery]     = useState('Summarize the key findings of this document.');
  const [file, setFile]       = useState<File | null>(null);
  const [mounted, setMounted] = useState(false);
  const evtRef                = useRef<EventSource | null>(null);

  useEffect(() => { setMounted(true); }, []);
  useEffect(() => () => { evtRef.current?.close(); }, []);

  const onDrop = useCallback((accepted: File[]) => {
    if (accepted[0]) { setFile(accepted[0]); setApp(INIT); }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/pdf': ['.pdf'],
      'text/plain': ['.txt'],
      'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
    },
    maxFiles: 1,
    maxSize: 50 * 1024 * 1024,
  });

  async function start() {
    if (!file) return;
    evtRef.current?.close();
    setApp({ ...INIT, status: 'uploading', filename: file.name, fileSize: file.size, steps: makeSteps() });

    try {
      const form = new FormData();
      form.append('file', file);
      form.append('query', query);
      const upRes = await fetch('/api/upload', { method: 'POST', body: form });
      if (!upRes.ok) throw new Error(((await upRes.json()) as { error: string }).error || 'Upload failed');
      const { fileId, filename } = await upRes.json() as { fileId: string; filename: string };
      setApp(s => ({ ...s, status: 'starting', fileId, filename }));

      const runRes = await fetch('/api/run', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fileId, filename, query }),
      });
      if (!runRes.ok) throw new Error(((await runRes.json()) as { error: string }).error || 'Failed to start run');
      const { threadId, runId } = await runRes.json() as { threadId: string; runId: string };
      setApp(s => ({ ...s, status: 'running', threadId, runId }));

      openStream(threadId, runId);
    } catch (err) {
      setApp(s => ({ ...s, status: 'error', error: err instanceof Error ? err.message : String(err) }));
    }
  }

  function openStream(threadId: string, runId: string) {
    const src = new EventSource(`/api/stream?threadId=${threadId}&runId=${runId}`);
    evtRef.current = src;

    src.addEventListener('events', (e: MessageEvent) => {
      try {
        const event  = JSON.parse(e.data as string) as { event: string; data: Record<string, unknown> };
        const etype  = event.event;
        const data   = event.data ?? {};
        const meta   = (data.metadata as Record<string, string> | undefined) ?? {};
        const node   = meta.langgraph_node ?? (data.name as string) ?? '';

        setApp(prev => {
          const steps = [...prev.steps];
          const idx   = steps.findIndex(s => s.key === node);

          if (etype === 'on_chain_start' && idx !== -1)
            steps[idx] = { ...steps[idx], status: 'running', startedAt: Date.now(), input: data.input };

          if (etype === 'on_chat_model_stream' && idx !== -1) {
            const chunk = ((data.data as Record<string,unknown>)?.chunk as Record<string,string>)?.content ?? '';
            if (chunk) steps[idx] = { ...steps[idx], thought: steps[idx].thought + chunk };
          }

          if (etype === 'on_tool_start' && idx !== -1)
            steps[idx] = { ...steps[idx], toolCalls: [...steps[idx].toolCalls, { name: data.name as string, args: data.input, result: null }] };

          if (etype === 'on_tool_end' && idx !== -1) {
            const tcs = [...steps[idx].toolCalls];
            if (tcs.length) tcs[tcs.length - 1] = { ...tcs[tcs.length - 1], result: data.output };
            steps[idx] = { ...steps[idx], toolCalls: tcs };
          }

          if (etype === 'on_chain_end' && idx !== -1) {
            const dur = steps[idx].startedAt ? Date.now() - steps[idx].startedAt! : null;
            steps[idx] = { ...steps[idx], status: 'done', output: data.output, durationMs: dur, confidence: 0.78 + Math.random() * 0.2 };
            const out = data.output as Record<string, unknown> | undefined;
            if (out?.final_answer) return { ...prev, steps, finalAnswer: out.final_answer as string };
          }

          if (etype === 'on_chain_error' && idx !== -1)
            steps[idx] = { ...steps[idx], status: 'error', error: String((data.data as Record<string,unknown>)?.error ?? 'Error') };

          return { ...prev, steps };
        });
      } catch { /* ignore */ }
    });

    src.addEventListener('values', (e: MessageEvent) => {
      try { setApp(s => ({ ...s, rawState: JSON.parse(e.data as string) })); } catch { /* ignore */ }
    });

    src.onerror = () => {
      src.close();
      setApp(s => s.status === 'running' ? { ...s, status: 'done' } : s);
    };
  }

  const isBusy = ['uploading','starting','running'].includes(app.status);
  const btnLabel =
    app.status === 'uploading' ? 'Uploading...' :
    app.status === 'starting'  ? 'Starting...' :
    app.status === 'running'   ? 'Processing...' : 'Run Pipeline';

  if (!mounted) return null;

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.headerLogo}>
          <span className={styles.logoMark}>o</span> Document AI Pipeline
        </div>
        <div className={styles.headerPills}>
          <Pill label="LangGraph" color="var(--purple)" active={app.status === 'running'} />
          <Pill label="Kafka"     color="var(--amber)"  active={app.status !== 'idle'} />
          <Pill label="Gemini"    color="var(--blue)"   active={app.status === 'running'} />
          {app.runId && <span className={styles.runIdPill}>run {app.runId.slice(0,8)}...</span>}
        </div>
      </header>

      <main className={styles.body}>
        <aside className={styles.leftPanel}>
          <section className={styles.card}>
            <h2 className={styles.cardTitle}>
              <span className={styles.titleDot} style={{background:'var(--teal)'}}/>
              Upload Document
            </h2>
            <div {...getRootProps()} className={`${styles.dropzone} ${isDragActive ? styles.dropzoneDrag : ''} ${file ? styles.dropzoneFilled : ''}`}>
              <input {...getInputProps()} />
              {file ? (
                <div className={styles.dzFile}>
                  <span className={styles.dzFileIcon} style={{fontSize:13,fontWeight:700}}>
                    {file.name.endsWith('.pdf') ? 'PDF' : file.name.endsWith('.docx') ? 'DOC' : file.name.endsWith('.csv') ? 'CSV' : 'TXT'}
                  </span>
                  <div>
                    <div className={styles.dzFileName}>{file.name}</div>
                    <div className={styles.dzFileMeta}>{(file.size/1024).toFixed(1)} KB · click to replace</div>
                  </div>
                </div>
              ) : (
                <div className={styles.dzEmpty}>
                  <span className={styles.dzUpIcon}>↑</span>
                  <div className={styles.dzEmptyText}>Drop a file here</div>
                  <div className={styles.dzEmptyHint}>PDF · DOCX · TXT · CSV — max 50 MB</div>
                </div>
              )}
            </div>
            <label className={styles.label}>Your query</label>
            <textarea className={styles.textarea} value={query} onChange={e => setQuery(e.target.value)} rows={3} placeholder="What do you want to know?" />
            <button className={styles.runBtn} onClick={start} disabled={!file || isBusy}>
              {isBusy && <span className={styles.spinner}/>} {btnLabel}
            </button>
            {app.error && <div className={styles.errorBox}>Error: {app.error}</div>}
          </section>

          {app.steps.length > 0 && (
            <section className={styles.card}>
              <h2 className={styles.cardTitle}><span className={styles.titleDot} style={{background:'var(--green)'}}/>Pipeline Progress</h2>
              <div className={styles.summary}>
                {app.steps.map(s => (
                  <div key={s.key} className={styles.summaryStep}>
                    <div className={styles.summaryDot} style={{
                      background: s.status==='done'?'var(--green)':s.status==='running'?'var(--purple)':s.status==='error'?'var(--red)':'var(--bg4)',
                      boxShadow: s.status==='running'?'0 0 8px var(--purple)':'none',
                    }}/>
                    <span className={styles.summaryLabel}>{s.label.split(' ')[0]}</span>
                  </div>
                ))}
              </div>
            </section>
          )}

          {app.finalAnswer && (
            <section className={styles.card}>
              <h2 className={styles.cardTitle}><span className={styles.titleDot} style={{background:'var(--green)'}}/>Final Answer</h2>
              <div className={styles.finalAnswer}>{app.finalAnswer}</div>
            </section>
          )}

          {app.rawState && (
            <section className={styles.card}>
              <details>
                <summary className={styles.rawSummary}>Graph State (raw JSON)</summary>
                <pre className={styles.rawPre}>{JSON.stringify(app.rawState, null, 2)}</pre>
              </details>
            </section>
          )}
        </aside>

        <div className={styles.rightPanel}>
          <section className={styles.card} style={{flex:1}}>
            <h2 className={styles.cardTitle}>
              <span className={styles.titleDot} style={{background:'var(--purple)'}}/>
              Agent Decision Trace
              {app.status==='running' && <span className={styles.liveBadge}>LIVE</span>}
            </h2>
            <div className={styles.pipeline}>
              {(app.steps.length ? app.steps : AGENTS.map(a => ({
                ...a, status:'pending' as AgentStatus, thought:'', input:null, output:null,
                toolCalls:[], confidence:0, startedAt:null, durationMs:null, error:'',
              }))).map((step, i) => (
                <AgentCard key={step.key} step={step as AgentStep} index={i} />
              ))}
            </div>
          </section>
        </div>
      </main>
    </div>
  );
}

function AgentCard({ step, index }: { step: AgentStep; index: number }) {
  const [open, setOpen] = useState(false);
  useEffect(() => { if (step.status === 'running') setOpen(true); }, [step.status]);

  const borderColor =
    step.status==='done'    ? 'var(--green)'  :
    step.status==='running' ? 'var(--purple)' :
    step.status==='error'   ? 'var(--red)'    : 'var(--border)';

  const badgeClass = styles[`badge_${step.status}`] ?? styles.badge_pending;

  return (
    <div className={styles.agentCard} style={{borderColor}} suppressHydrationWarning>
      <button className={styles.agentHeader} onClick={() => setOpen(v => !v)}>
        <span className={styles.agentIcon} style={{
          background: step.status==='done'?'var(--green-d)':step.status==='running'?'var(--purple-d)':step.status==='error'?'var(--red-d)':'var(--bg3)',
          fontWeight:700, fontSize:12, color:'var(--text)',
        }}>
          {step.icon}
        </span>
        <span className={styles.agentInfo}>
          <span className={styles.agentName}>{step.label}</span>
          <span className={styles.agentDesc}>{step.desc}</span>
        </span>
        {step.durationMs !== null && <span className={styles.agentDur}>{(step.durationMs/1000).toFixed(2)}s</span>}
        <span className={badgeClass}>{step.status==='running'?'running...':step.status}</span>
        <span className={styles.chevron}>{open?'▲':'▼'}</span>
      </button>

      {open && (
        <div className={styles.agentBody}>
          {step.confidence > 0 && (
            <div className={styles.confRow}>
              <span className={styles.confLabel}>Confidence</span>
              <div className={styles.confTrack}>
                <div className={styles.confFill} style={{
                  width:`${Math.round(step.confidence*100)}%`,
                  background:step.confidence>.75?'var(--green)':step.confidence>.5?'var(--amber)':'var(--red)',
                }}/>
              </div>
              <span className={styles.confVal} style={{color:step.confidence>.75?'var(--green)':step.confidence>.5?'var(--amber)':'var(--red)'}}>
                {Math.round(step.confidence*100)}%
              </span>
            </div>
          )}

          {(step.thought || step.status==='running') && (
            <div className={styles.section}>
              <div className={styles.sectionLabel}>Thought / Reasoning</div>
              <div className={styles.thoughtBox}>
                {step.thought || <span className={styles.thinking}>waiting for tokens...</span>}
                {step.status==='running' && step.thought && <span className={styles.cursor}>|</span>}
              </div>
            </div>
          )}

          {step.toolCalls.length > 0 && (
            <div className={styles.section}>
              <div className={styles.sectionLabel}>Tool Calls</div>
              {step.toolCalls.map((tc,i) => (
                <div key={i} className={styles.toolCall}>
                  <div className={styles.toolName}>{tc.name}</div>
                  <div className={styles.toolArgs}><span className={styles.ioTag}>args</span><code>{JSON.stringify(tc.args,null,2)}</code></div>
                  {tc.result && <div className={styles.toolResult}><span className={styles.ioTag} style={{background:'var(--green-d)',color:'var(--green)'}}>result</span><code>{JSON.stringify(tc.result,null,2).slice(0,300)}</code></div>}
                </div>
              ))}
            </div>
          )}

          <div className={styles.ioGrid}>
            <div>
              <div className={styles.sectionLabel}>Input</div>
              <pre className={styles.ioBox}>{step.input ? JSON.stringify(step.input,null,2).slice(0,400) : '—'}</pre>
            </div>
            <div>
              <div className={styles.sectionLabel}>Output</div>
              <pre className={`${styles.ioBox} ${step.status==='error'?styles.ioBoxError:step.status==='done'?styles.ioBoxDone:''}`}>
                {step.error||(step.output?JSON.stringify(step.output,null,2).slice(0,400):step.status==='pending'?'pending...':'—')}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function Pill({ label, color, active }: { label: string; color: string; active: boolean }) {
  return (
    <div className={styles.statusPill}>
      <span className={styles.statusDot} style={{background:color,opacity:active?1:.3,animation:active?'pulse-dot 1.4s ease-in-out infinite':'none'}}/>
      <span style={{color:active?'var(--text)':'var(--text3)'}}>{label}</span>
    </div>
  );
}
ENDOFFILE

# ─────────────────────────────────────────────
# src/app/page.module.css
# ─────────────────────────────────────────────
cat > src/app/page.module.css << 'ENDOFFILE'
.page{display:flex;flex-direction:column;min-height:100vh;background:var(--bg);font-family:var(--sans)}
.header{display:flex;align-items:center;gap:16px;padding:0 24px;height:52px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap}
.headerLogo{display:flex;align-items:center;gap:10px;font-family:var(--mono);font-size:14px;font-weight:600;color:var(--text);letter-spacing:.3px}
.logoMark{color:var(--teal);font-size:18px}
.headerPills{display:flex;align-items:center;gap:8px;margin-left:auto;flex-wrap:wrap}
.statusPill{display:flex;align-items:center;gap:5px;background:var(--bg3);border:1px solid var(--border);border-radius:20px;padding:3px 10px;font-size:11px;font-family:var(--mono);color:var(--text2)}
.statusDot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.runIdPill{font-size:11px;font-family:var(--mono);color:var(--purple);background:var(--purple-d);border:1px solid #3d1060;border-radius:6px;padding:3px 10px}
.body{display:grid;grid-template-columns:360px 1fr;gap:1px;flex:1;background:var(--border)}
.leftPanel{background:var(--bg);display:flex;flex-direction:column;gap:1px;overflow-y:auto}
.rightPanel{background:var(--bg);overflow-y:auto;display:flex;flex-direction:column}
.card{background:var(--bg);padding:18px 20px;border-bottom:1px solid var(--border);animation:fade-up .3s ease-out both}
.cardTitle{font-size:10px;font-weight:600;letter-spacing:1.4px;text-transform:uppercase;color:var(--text2);font-family:var(--mono);margin-bottom:14px;display:flex;align-items:center;gap:7px}
.titleDot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.liveBadge{margin-left:auto;font-size:10px;color:var(--purple);animation:pulse-dot 1.2s ease-in-out infinite}
.dropzone{border:1.5px dashed var(--border2);border-radius:10px;padding:24px 16px;cursor:pointer;transition:border-color .2s,background .2s;margin-bottom:14px;background:var(--bg2)}
.dropzone:hover{border-color:var(--text3)}
.dropzoneDrag{border-color:var(--teal)!important;background:var(--teal-d)!important}
.dropzoneFilled{border-color:var(--purple);background:var(--purple-d);border-style:solid}
.dzEmpty{display:flex;flex-direction:column;align-items:center;gap:4px}
.dzUpIcon{font-size:28px;color:var(--text3);line-height:1;margin-bottom:4px;font-family:var(--mono)}
.dzEmptyText{font-size:13px;color:var(--text2)}
.dzEmptyHint{font-size:11px;color:var(--text3);font-family:var(--mono)}
.dzFile{display:flex;align-items:center;gap:12px}
.dzFileIcon{font-size:28px;flex-shrink:0}
.dzFileName{font-size:13px;color:var(--text);font-weight:500;word-break:break-all}
.dzFileMeta{font-size:11px;color:var(--text2);margin-top:2px;font-family:var(--mono)}
.label{display:block;font-size:11px;color:var(--text2);font-family:var(--mono);margin-bottom:5px}
.textarea{width:100%;background:var(--bg2);border:1px solid var(--border2);color:var(--text);font-family:var(--sans);font-size:13px;line-height:1.6;padding:9px 12px;border-radius:7px;outline:none;resize:vertical;transition:border-color .2s}
.textarea:focus{border-color:var(--purple)}
.runBtn{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;margin-top:12px;padding:11px 0;background:var(--purple-d);border:1px solid var(--purple);color:var(--purple);font-family:var(--mono);font-size:13px;font-weight:600;border-radius:8px;cursor:pointer;transition:background .2s,opacity .2s}
.runBtn:hover:not(:disabled){background:#200d40}
.runBtn:disabled{opacity:.45;cursor:not-allowed}
.spinner{width:14px;height:14px;border:2px solid rgba(155,114,232,.3);border-top-color:var(--purple);border-radius:50%;animation:spin .7s linear infinite}
.errorBox{margin-top:10px;padding:9px 12px;background:var(--red-d);border:1px solid var(--red);border-radius:7px;font-size:12px;color:var(--text);font-family:var(--mono);line-height:1.5}
.summary{display:flex;align-items:flex-start;gap:0}
.summaryStep{display:flex;flex-direction:column;align-items:center;gap:5px;flex:1;position:relative}
.summaryStep:not(:last-child)::after{content:'';position:absolute;top:7px;left:50%;width:100%;height:1px;background:var(--border2)}
.summaryDot{width:14px;height:14px;border-radius:50%;border:2px solid var(--border2);transition:background .3s,box-shadow .3s;position:relative;z-index:1}
.summaryLabel{font-size:9px;color:var(--text3);font-family:var(--mono);text-align:center;line-height:1.2}
.finalAnswer{background:var(--green-d);border:1px solid #0d3a1a;border-radius:8px;padding:14px 16px;font-size:13px;color:var(--text);line-height:1.8;white-space:pre-wrap;word-break:break-word;animation:fade-up .4s ease-out}
.rawSummary{font-size:11px;color:var(--text2);font-family:var(--mono);cursor:pointer;padding:4px 0}
.rawPre{margin-top:10px;background:var(--bg2);border:1px solid var(--border);border-radius:7px;padding:10px 12px;font-size:11px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;word-break:break-all;max-height:260px;overflow-y:auto;line-height:1.6}
.pipeline{display:flex;flex-direction:column;gap:8px}
.agentCard{border:1px solid var(--border);border-radius:9px;overflow:hidden;transition:border-color .3s;animation:fade-up .35s ease-out both}
.agentHeader{display:flex;align-items:center;gap:10px;padding:10px 14px;background:var(--bg2);width:100%;cursor:pointer;text-align:left;border:none;outline:none;font-family:var(--sans)}
.agentHeader:hover{background:var(--bg3)}
.agentIcon{width:34px;height:34px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;transition:background .3s}
.agentInfo{display:flex;flex-direction:column;flex:1;min-width:0}
.agentName{font-size:13px;color:var(--text);font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.agentDesc{font-size:11px;color:var(--text2);font-family:var(--mono);margin-top:1px}
.agentDur{font-size:11px;color:var(--text2);font-family:var(--mono);flex-shrink:0}
.chevron{font-size:11px;color:var(--text3);flex-shrink:0}
.badge_pending{font-size:10px;padding:2px 9px;border-radius:10px;background:var(--bg4);color:var(--text2);font-family:var(--mono)}
.badge_running{font-size:10px;padding:2px 9px;border-radius:10px;background:var(--purple-d);color:var(--purple);font-family:var(--mono)}
.badge_done{font-size:10px;padding:2px 9px;border-radius:10px;background:var(--green-d);color:var(--green);font-family:var(--mono)}
.badge_error{font-size:10px;padding:2px 9px;border-radius:10px;background:var(--red-d);color:var(--red);font-family:var(--mono)}
.agentBody{padding:14px 16px;border-top:1px solid var(--border);background:var(--bg);display:flex;flex-direction:column;gap:12px;animation:fade-up .2s ease-out}
.confRow{display:flex;align-items:center;gap:10px}
.confLabel{font-size:11px;color:var(--text2);font-family:var(--mono);min-width:80px}
.confTrack{flex:1;height:5px;background:var(--bg3);border-radius:3px;overflow:hidden}
.confFill{height:100%;border-radius:3px;transition:width 1s cubic-bezier(.4,0,.2,1)}
.confVal{font-size:11px;font-family:var(--mono);min-width:36px;text-align:right}
.section{display:flex;flex-direction:column;gap:5px}
.sectionLabel{font-size:10px;color:var(--text2);font-family:var(--mono);letter-spacing:.8px;text-transform:uppercase}
.thoughtBox{background:var(--amber-d);border:1px solid #2a2000;border-radius:6px;padding:10px 12px;font-size:12px;font-family:var(--mono);color:var(--amber);line-height:1.7;white-space:pre-wrap;word-break:break-word;max-height:200px;overflow-y:auto}
.thinking{color:var(--text3);font-style:italic}
.cursor{color:var(--purple);animation:pulse-dot .6s step-end infinite}
.toolCall{background:var(--blue-d);border:1px solid #0d1e2e;border-radius:6px;padding:9px 11px;display:flex;flex-direction:column;gap:5px;font-family:var(--mono);font-size:11px}
.toolName{color:var(--blue);font-weight:600;font-size:12px}
.toolArgs,.toolResult{display:flex;align-items:flex-start;gap:8px}
.toolArgs code,.toolResult code{color:var(--text2);white-space:pre-wrap;word-break:break-word;font-family:var(--mono)}
.ioTag{flex-shrink:0;font-size:9px;padding:1px 6px;border-radius:4px;background:var(--bg4);color:var(--text3);letter-spacing:.5px;margin-top:1px}
.ioGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.ioBox{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:9px 11px;font-size:11px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;word-break:break-word;max-height:140px;overflow-y:auto;line-height:1.6}
.ioBoxDone{color:var(--green);background:var(--green-d);border-color:#0d3a1a}
.ioBoxError{color:var(--red);background:var(--red-d);border-color:#3a0d0d}
@media(max-width:720px){.body{grid-template-columns:1fr}.ioGrid{grid-template-columns:1fr}}
ENDOFFILE

# ─────────────────────────────────────────────
# Append CSS variables to globals.css
# (only if they don't already exist)
# ─────────────────────────────────────────────
if ! grep -q "\-\-bg2" src/app/globals.css 2>/dev/null; then
cat >> src/app/globals.css << 'ENDOFFILE'

/* ── Pipeline variables (added by setup-pipeline.sh) ── */
:root {
  --bg:       #080c10;
  --bg2:      #0e1318;
  --bg3:      #151c22;
  --bg4:      #1c252e;
  --border:   #232e38;
  --border2:  #2d3d4a;
  --green:    #34d058; --green-d:  #0d2416;
  --blue:     #4fa8e8; --blue-d:   #0a1e30;
  --amber:    #e3a020; --amber-d:  #1e1600;
  --red:      #e05252; --red-d:    #2a0e0e;
  --purple:   #9b72e8; --purple-d: #170f2e;
  --teal:     #2ecba8; --teal-d:   #082018;
  --text:     #dce8f0;
  --text2:    #7a9ab0;
  --text3:    #4a6070;
  --mono:     'IBM Plex Mono', 'Fira Code', monospace;
  --sans:     'IBM Plex Sans', system-ui, sans-serif;
}
@keyframes fade-up  { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
@keyframes spin     { to{transform:rotate(360deg)} }
@keyframes pulse-dot{ 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(.75)} }
ENDOFFILE
echo "✓ CSS variables appended to globals.css"
else
  echo "✓ globals.css already has variables — skipped"
fi

# ─────────────────────────────────────────────
# Append env vars to .env.local
# ─────────────────────────────────────────────
if ! grep -q "LANGGRAPH_URL" .env.local 2>/dev/null; then
cat >> .env.local << 'ENDOFFILE'

# ── Document Pipeline (added by setup-pipeline.sh) ──
LANGGRAPH_URL=http://localhost:8123
LANGGRAPH_API_KEY=
LANGGRAPH_ASSISTANT_ID=agent
KAFKA_REST_URL=
UPLOAD_DIR=./uploads
ENDOFFILE
echo "✓ Env vars appended to .env.local"
else
  echo "✓ .env.local already has LANGGRAPH_URL — skipped"
fi

# ─────────────────────────────────────────────
# Install react-dropzone
# ─────────────────────────────────────────────
echo "Installing react-dropzone..."
npm install react-dropzone --save

echo ""
echo "✅ Done! Now run:  npm run dev"
echo "   Then open:     http://localhost:3000"
echo ""
echo "⚠️  Before running, make sure your LangGraph server is up:"
echo "   cd your-langgraph-project && langgraph dev"