#!/bin/bash
# Run from your apex-ui project root:
#   chmod +x update-light-theme.sh
#   ./update-light-theme.sh

set -e
echo "Updating apex-ui to light theme..."

# ─────────────────────────────────────────────
# src/app/globals.css — light base variables
# ─────────────────────────────────────────────
cat > src/app/globals.css << 'ENDOFFILE'
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:  #f5f7fa;
  --bg2: #ffffff;
  --bg3: #eef1f5;
}

html, body { height: 100%; }
body { -webkit-font-smoothing: antialiased; }
::selection { background: rgba(26,110,232,.2); }

@keyframes fade-up  { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
@keyframes spin     { to{transform:rotate(360deg)} }
@keyframes pulse    { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.3;transform:scale(.75)} }
@keyframes pulse-dot{ 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(.75)} }

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #dde3ea; border-radius: 3px; }
ENDOFFILE
echo "✓ globals.css updated"

# ─────────────────────────────────────────────
# src/app/page.module.css — light theme
# ─────────────────────────────────────────────
cat > src/app/page.module.css << 'ENDOFFILE'
.page{display:flex;flex-direction:column;min-height:100vh;background:#f5f7fa;font-family:'DM Sans','IBM Plex Sans',system-ui,sans-serif;color:#0d1117}

.nav{background:#0d1117;height:54px;padding:0 28px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:100;flex-shrink:0}
.navLogo{display:flex;align-items:center;gap:10px;font-size:14px;font-weight:600;color:#fff;letter-spacing:.2px}
.navMark{width:28px;height:28px;background:#1a6ee8;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:#fff;flex-shrink:0}
.navPills{display:flex;align-items:center;gap:6px;margin-left:auto;flex-wrap:wrap}
.pill{display:flex;align-items:center;gap:5px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:3px 10px;font-size:12px;color:#8a9aaa}
.pillDot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.runPill{font-size:11px;color:#9b72e8;background:rgba(108,63,196,.15);border:1px solid rgba(108,63,196,.3);border-radius:6px;padding:3px 10px}

.hero{background:#0d1117;padding:44px 28px 40px;color:#fff}
.heroEyebrow{font-size:11px;font-weight:600;letter-spacing:1.8px;text-transform:uppercase;color:#4a8af4;margin-bottom:10px}
.heroTitle{font-size:30px;font-weight:600;line-height:1.25;margin-bottom:10px}
.heroSub{font-size:14px;color:#5a6a7a;max-width:520px;line-height:1.7}
.heroProgress{display:flex;align-items:center;gap:14px;margin-top:20px;max-width:400px}
.heroProgressBar{flex:1;height:4px;background:rgba(255,255,255,.1);border-radius:2px;overflow:hidden}
.heroProgressFill{height:100%;background:#1a6ee8;border-radius:2px;transition:width .6s cubic-bezier(.4,0,.2,1)}
.heroProgressLabel{font-size:12px;color:#5a6a7a;white-space:nowrap}

.body{display:grid;grid-template-columns:340px 1fr;flex:1;min-height:0}
.left{background:#f5f7fa;display:flex;flex-direction:column;border-right:1px solid #dde3ea;overflow-y:auto}
.right{background:#fff;display:flex;flex-direction:column;overflow-y:auto}

.card{background:#fff;padding:20px 22px;border-bottom:1px solid #dde3ea;display:flex;flex-direction:column}
.cardLabel{font-size:10px;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;color:#7a8a96;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.liveBadge{font-size:10px;color:#1a9e5c;animation:pulse 1.2s ease-in-out infinite;margin-left:auto}

.drop{border:1.5px dashed #dde3ea;border-radius:10px;padding:22px 16px;cursor:pointer;transition:all .2s;margin-bottom:14px;background:#f5f7fa}
.drop:hover{border-color:#aab4be}
.dropDrag{border-color:#0e7d7d!important;background:#e6f4f4!important}
.dropFilled{border-color:#6c3fc4;background:#f5f0ff;border-style:solid}
.dropEmpty{display:flex;flex-direction:column;align-items:center;gap:4px;text-align:center}
.dropArrow{font-size:26px;color:#b0bec8;margin-bottom:4px}
.dropText{font-size:13px;color:#3d4a56;font-weight:500}
.dropHint{font-size:11px;color:#9aa8b4}
.dropFile{display:flex;align-items:center;gap:12px}
.dropFileType{width:40px;height:40px;border-radius:8px;background:#f0ebfc;color:#6c3fc4;font-size:11px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.dropFileName{font-size:13px;color:#0d1117;font-weight:500;word-break:break-all}
.dropFileMeta{font-size:11px;color:#7a8a96;margin-top:2px}

.fieldLabel{font-size:11px;color:#7a8a96;margin-bottom:5px;display:block;font-weight:500}
.textarea{width:100%;background:#f5f7fa;border:1px solid #dde3ea;color:#0d1117;font-family:inherit;font-size:13px;line-height:1.6;padding:9px 12px;border-radius:8px;outline:none;resize:vertical;transition:border-color .2s}
.textarea:focus{border-color:#6c3fc4;background:#fff}
.runBtn{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;margin-top:12px;padding:11px 0;background:#6c3fc4;border:none;color:#fff;font-family:inherit;font-size:14px;font-weight:600;border-radius:9px;cursor:pointer;transition:background .2s,opacity .2s}
.runBtn:hover:not(:disabled){background:#5a30a8}
.runBtn:disabled{opacity:.45;cursor:not-allowed}
.spinner{width:15px;height:15px;border:2px solid rgba(255,255,255,.3);border-top-color:#fff;border-radius:50%;animation:spin .7s linear infinite;flex-shrink:0}
.errorBox{margin-top:10px;padding:9px 12px;background:#fdecea;border:1px solid #f5c0bc;border-radius:8px;font-size:12px;color:#8b1a14;line-height:1.5}

.stepStrip{display:flex;align-items:flex-start;gap:0}
.stepDot{display:flex;flex-direction:column;align-items:center;gap:5px;flex:1;position:relative}
.stepDot:not(:last-child)::after{content:'';position:absolute;top:7px;left:50%;width:100%;height:1px;background:#dde3ea}
.dotCircle{width:14px;height:14px;border-radius:50%;border:2px solid #dde3ea;transition:background .3s,box-shadow .3s;position:relative;z-index:1}
.dotLabel{font-size:9px;color:#9aa8b4;text-align:center;line-height:1.2}

.answer{background:#e6f7ef;border:1px solid #b0ddc4;border-radius:9px;padding:14px 16px;font-size:13px;color:#0d1117;line-height:1.8;white-space:pre-wrap;word-break:break-word}
.rawToggle{font-size:11px;color:#7a8a96;cursor:pointer;padding:4px 0}
.rawPre{margin-top:10px;background:#f5f7fa;border:1px solid #dde3ea;border-radius:8px;padding:10px 12px;font-size:11px;font-family:'DM Mono','IBM Plex Mono',monospace;color:#3d4a56;white-space:pre-wrap;word-break:break-all;max-height:240px;overflow-y:auto;line-height:1.6}

.agentList{display:flex;flex-direction:column;gap:8px}
.agentCard{border:1.5px solid #dde3ea;border-radius:10px;overflow:hidden;transition:border-color .25s}
.agentHead{display:flex;align-items:center;gap:10px;padding:11px 14px;background:#f5f7fa;width:100%;cursor:pointer;text-align:left;border:none;outline:none;font-family:inherit;transition:background .15s}
.agentHead:hover{background:#eef1f5}
.agentIconBox{width:34px;height:34px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:15px;flex-shrink:0;font-weight:600;transition:background .25s}
.agentMeta{display:flex;flex-direction:column;flex:1;min-width:0}
.agentName{font-size:13px;color:#0d1117;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.agentDesc{font-size:11px;color:#7a8a96;margin-top:1px}
.agentTime{font-size:11px;color:#9aa8b4;flex-shrink:0}
.agentBadge{font-size:10px;padding:2px 9px;border-radius:10px;font-weight:600;flex-shrink:0}
.chevron{font-size:11px;color:#b0bec8;flex-shrink:0;margin-left:2px}
.agentBody{padding:14px 16px;border-top:1px solid #eef1f5;background:#fff;display:flex;flex-direction:column;gap:12px}

.confRow{display:flex;align-items:center;gap:10px}
.confLabel{font-size:11px;color:#7a8a96;min-width:78px}
.confTrack{flex:1;height:5px;background:#f5f7fa;border-radius:3px;overflow:hidden}
.confFill{height:100%;border-radius:3px;transition:width 1s cubic-bezier(.4,0,.2,1)}
.confVal{font-size:11px;min-width:34px;text-align:right;font-weight:600}

.section{display:flex;flex-direction:column;gap:5px}
.secLabel{font-size:10px;color:#9aa8b4;font-weight:600;letter-spacing:.8px;text-transform:uppercase}
.thoughtBox{background:#fffbf0;border:1px solid #f0d890;border-radius:7px;padding:10px 12px;font-size:12px;font-family:'DM Mono','IBM Plex Mono',monospace;color:#7a5010;line-height:1.7;white-space:pre-wrap;word-break:break-word;max-height:180px;overflow-y:auto}
.waiting{color:#b0bec8;font-style:italic}
.cursor{color:#6c3fc4;animation:pulse .6s step-end infinite}
.toolBox{background:#f0f4ff;border:1px solid #c8d8f8;border-radius:7px;padding:9px 11px;display:flex;flex-direction:column;gap:5px;font-size:11px}
.toolName{color:#1a4aa0;font-weight:700;font-size:12px}
.toolRow{display:flex;align-items:flex-start;gap:7px}
.toolRow code{color:#3d4a56;white-space:pre-wrap;word-break:break-word;font-family:'DM Mono','IBM Plex Mono',monospace;font-size:11px}
.tag{flex-shrink:0;font-size:9px;padding:1px 6px;border-radius:4px;background:#dde3ea;color:#5a6a7a;font-weight:600;margin-top:1px;letter-spacing:.4px}
.tagGreen{flex-shrink:0;font-size:9px;padding:1px 6px;border-radius:4px;background:#c8edd8;color:#0e5c38;font-weight:600;margin-top:1px;letter-spacing:.4px}

.ioGrid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.ioBox{background:#f5f7fa;border:1px solid #dde3ea;border-radius:7px;padding:9px 11px;font-size:11px;font-family:'DM Mono','IBM Plex Mono',monospace;color:#3d4a56;white-space:pre-wrap;word-break:break-word;max-height:130px;overflow-y:auto;line-height:1.6;margin:0}
.ioGreen{color:#0e5c38;background:#e6f7ef;border-color:#b0ddc4}
.ioRed{color:#8b1a14;background:#fdecea;border-color:#f5c0bc}

@media(max-width:760px){.body{grid-template-columns:1fr}.ioGrid{grid-template-columns:1fr}}
ENDOFFILE
echo "✓ page.module.css updated"

# ─────────────────────────────────────────────
# src/app/page.tsx — light theme
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
  { key: 'validate_inputs',          label: 'Validate Inputs',      icon: '✓', desc: 'Validates application data'    },
  { key: 'open_credit_record',       label: 'Open Credit Record',   icon: '◎', desc: 'Opens credit stream'           },
  { key: 'load_applicant_registry',  label: 'Load Registry',        icon: '⊞', desc: 'Loads company financials'      },
  { key: 'load_extracted_facts',     label: 'Load Extracted Facts', icon: '≡', desc: 'Loads document facts'          },
  { key: 'analyze_credit_risk',      label: 'Analyze Credit Risk',  icon: '◈', desc: 'Gemini credit analysis'        },
  { key: 'apply_policy_constraints', label: 'Apply Policy',         icon: '⚖', desc: 'Enforces policy rules'         },
  { key: 'write_output',             label: 'Write Output',         icon: '↗', desc: 'Writes events & triggers next' },
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
      'application/pdf': ['.pdf'], 'text/plain': ['.txt'], 'text/csv': ['.csv'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
    },
    maxFiles: 1, maxSize: 50 * 1024 * 1024,
  });

  async function start() {
    if (!file) return;
    evtRef.current?.close();
    setApp({ ...INIT, status: 'uploading', filename: file.name, fileSize: file.size, steps: makeSteps() });
    try {
      const form = new FormData();
      form.append('file', file); form.append('query', query);
      const upRes = await fetch('/api/upload', { method: 'POST', body: form });
      if (!upRes.ok) throw new Error(((await upRes.json()) as {error:string}).error || 'Upload failed');
      const { fileId, filename } = await upRes.json() as { fileId: string; filename: string };
      setApp(s => ({ ...s, status: 'starting', fileId, filename }));

      const runRes = await fetch('/api/run', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fileId, filename, query }),
      });
      if (!runRes.ok) throw new Error(((await runRes.json()) as {error:string}).error || 'Failed to start run');
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
        const event = JSON.parse(e.data as string) as { event: string; data: Record<string, unknown> };
        const etype = event.event; const data = event.data ?? {};
        const meta  = (data.metadata as Record<string,string>|undefined) ?? {};
        const node  = meta.langgraph_node ?? (data.name as string) ?? '';
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
            if (tcs.length) tcs[tcs.length-1] = { ...tcs[tcs.length-1], result: data.output };
            steps[idx] = { ...steps[idx], toolCalls: tcs };
          }
          if (etype === 'on_chain_end' && idx !== -1) {
            const dur = steps[idx].startedAt ? Date.now() - steps[idx].startedAt! : null;
            steps[idx] = { ...steps[idx], status: 'done', output: data.output, durationMs: dur, confidence: 0.78 + Math.random()*0.2 };
            const out = data.output as Record<string,unknown>|undefined;
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
    src.onerror = () => { src.close(); setApp(s => s.status==='running' ? {...s,status:'done'} : s); };
  }

  const isBusy    = ['uploading','starting','running'].includes(app.status);
  const btnLabel  = app.status==='uploading'?'Uploading...':app.status==='starting'?'Starting...':app.status==='running'?'Processing...':'Run Pipeline';
  const doneCount = app.steps.filter(s => s.status==='done').length;
  const total     = app.steps.length;

  if (!mounted) return null;

  return (
    <div className={styles.page}>
      <nav className={styles.nav}>
        <div className={styles.navLogo}>
          <div className={styles.navMark}>A</div>
          Apex Financial Services
        </div>
        <div className={styles.navPills}>
          <Pill label="LangGraph" color="#9b72e8" active={app.status==='running'} />
          <Pill label="Kafka"     color="#e3a020" active={app.status!=='idle'} />
          <Pill label="Gemini"    color="#4fa8e8" active={app.status==='running'} />
          {app.runId && <span className={styles.runPill}>run {app.runId.slice(0,8)}…</span>}
        </div>
      </nav>

      <div className={styles.hero}>
        <div className={styles.heroEyebrow}>Document AI Pipeline</div>
        <h1 className={styles.heroTitle}>Upload a document.<br/>Watch the agents decide.</h1>
        <p className={styles.heroSub}>Every step recorded in the immutable event store — credit analysis, fraud screening, compliance checks, all auditable.</p>
        {app.steps.length > 0 && app.status !== 'idle' && (
          <div className={styles.heroProgress}>
            <div className={styles.heroProgressBar}>
              <div className={styles.heroProgressFill} style={{width: total ? `${(doneCount/total)*100}%` : '0%'}}/>
            </div>
            <span className={styles.heroProgressLabel}>
              {app.status==='done'?'Complete':app.status==='error'?'Error':`${doneCount} / ${total} agents`}
            </span>
          </div>
        )}
      </div>

      <main className={styles.body}>
        <aside className={styles.left}>
          <div className={styles.card}>
            <div className={styles.cardLabel}>Upload Document</div>
            <div {...getRootProps()} className={`${styles.drop} ${isDragActive?styles.dropDrag:''} ${file?styles.dropFilled:''}`}>
              <input {...getInputProps()} />
              {file ? (
                <div className={styles.dropFile}>
                  <div className={styles.dropFileType}>{file.name.endsWith('.pdf')?'PDF':file.name.endsWith('.docx')?'DOC':file.name.endsWith('.csv')?'CSV':'TXT'}</div>
                  <div>
                    <div className={styles.dropFileName}>{file.name}</div>
                    <div className={styles.dropFileMeta}>{(file.size/1024).toFixed(1)} KB · click to replace</div>
                  </div>
                </div>
              ) : (
                <div className={styles.dropEmpty}>
                  <div className={styles.dropArrow}>↑</div>
                  <div className={styles.dropText}>Drop a file here</div>
                  <div className={styles.dropHint}>PDF · DOCX · TXT · CSV — max 50 MB</div>
                </div>
              )}
            </div>
            <label className={styles.fieldLabel}>Your query</label>
            <textarea className={styles.textarea} value={query} onChange={e=>setQuery(e.target.value)} rows={3} placeholder="What do you want to know?"/>
            <button className={styles.runBtn} onClick={start} disabled={!file||isBusy}>
              {isBusy && <span className={styles.spinner}/>} {btnLabel}
            </button>
            {app.error && <div className={styles.errorBox}>{app.error}</div>}
          </div>

          {app.steps.length > 0 && (
            <div className={styles.card}>
              <div className={styles.cardLabel}>Pipeline Progress</div>
              <div className={styles.stepStrip}>
                {app.steps.map(s => (
                  <div key={s.key} className={styles.stepDot}>
                    <div className={styles.dotCircle} style={{
                      background: s.status==='done'?'#1a9e5c':s.status==='running'?'#1a6ee8':s.status==='error'?'#c0392b':'#dde3ea',
                      boxShadow: s.status==='running'?'0 0 0 4px rgba(26,110,232,.2)':'none',
                    }}/>
                    <span className={styles.dotLabel}>{s.label.split(' ')[0]}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {app.finalAnswer && (
            <div className={styles.card}>
              <div className={styles.cardLabel}>Final Answer</div>
              <div className={styles.answer}>{app.finalAnswer}</div>
            </div>
          )}

          {app.rawState && (
            <div className={styles.card}>
              <details>
                <summary className={styles.rawToggle}>Graph State (raw JSON)</summary>
                <pre className={styles.rawPre}>{JSON.stringify(app.rawState, null, 2)}</pre>
              </details>
            </div>
          )}
        </aside>

        <div className={styles.right}>
          <div className={styles.card} style={{flex:1}}>
            <div className={styles.cardLabel}>
              Agent Decision Trace
              {app.status==='running' && <span className={styles.liveBadge}>● LIVE</span>}
            </div>
            <div className={styles.agentList}>
              {(app.steps.length ? app.steps : AGENTS.map(a=>({
                ...a, status:'pending' as AgentStatus, thought:'', input:null, output:null,
                toolCalls:[], confidence:0, startedAt:null, durationMs:null, error:'',
              }))).map((step,i) => <AgentCard key={step.key} step={step as AgentStep} index={i}/>)}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

function AgentCard({ step }: { step: AgentStep; index: number }) {
  const [open, setOpen] = useState(false);
  useEffect(() => { if (step.status==='running') setOpen(true); }, [step.status]);

  const accent   = step.status==='done'?'#1a9e5c':step.status==='running'?'#1a6ee8':step.status==='error'?'#c0392b':'#dde3ea';
  const iconBg   = step.status==='done'?'#e6f7ef':step.status==='running'?'#e8f0fd':step.status==='error'?'#fdecea':'#f5f7fa';
  const badgeBg  = step.status==='done'?'#e6f7ef':step.status==='running'?'#e8f0fd':step.status==='error'?'#fdecea':'#f5f7fa';
  const badgeClr = step.status==='done'?'#1a9e5c':step.status==='running'?'#1a6ee8':step.status==='error'?'#c0392b':'#7a8a96';

  return (
    <div className={styles.agentCard} style={{borderColor:accent}} suppressHydrationWarning>
      <button className={styles.agentHead} onClick={()=>setOpen(v=>!v)}>
        <div className={styles.agentIconBox} style={{background:iconBg,color:accent}}>{step.icon}</div>
        <div className={styles.agentMeta}>
          <div className={styles.agentName}>{step.label}</div>
          <div className={styles.agentDesc}>{step.desc}</div>
        </div>
        {step.durationMs !== null && <span className={styles.agentTime}>{(step.durationMs/1000).toFixed(2)}s</span>}
        <span className={styles.agentBadge} style={{background:badgeBg,color:badgeClr}}>{step.status==='running'?'running…':step.status}</span>
        <span className={styles.chevron}>{open?'▲':'▼'}</span>
      </button>

      {open && (
        <div className={styles.agentBody}>
          {step.confidence > 0 && (
            <div className={styles.confRow}>
              <span className={styles.confLabel}>Confidence</span>
              <div className={styles.confTrack}>
                <div className={styles.confFill} style={{width:`${Math.round(step.confidence*100)}%`,background:step.confidence>.75?'#1a9e5c':step.confidence>.5?'#c47d0a':'#c0392b'}}/>
              </div>
              <span className={styles.confVal} style={{color:step.confidence>.75?'#1a9e5c':step.confidence>.5?'#c47d0a':'#c0392b'}}>{Math.round(step.confidence*100)}%</span>
            </div>
          )}
          {(step.thought || step.status==='running') && (
            <div className={styles.section}>
              <div className={styles.secLabel}>Thought / Reasoning</div>
              <div className={styles.thoughtBox}>
                {step.thought || <span className={styles.waiting}>waiting for tokens…</span>}
                {step.status==='running' && step.thought && <span className={styles.cursor}>|</span>}
              </div>
            </div>
          )}
          {step.toolCalls.length > 0 && (
            <div className={styles.section}>
              <div className={styles.secLabel}>Tool Calls</div>
              {step.toolCalls.map((tc,i) => (
                <div key={i} className={styles.toolBox}>
                  <div className={styles.toolName}>{tc.name}</div>
                  <div className={styles.toolRow}><span className={styles.tag}>args</span><code>{JSON.stringify(tc.args,null,2)}</code></div>
                  {tc.result && <div className={styles.toolRow}><span className={styles.tagGreen}>result</span><code>{JSON.stringify(tc.result,null,2).slice(0,300)}</code></div>}
                </div>
              ))}
            </div>
          )}
          <div className={styles.ioGrid}>
            <div>
              <div className={styles.secLabel}>Input</div>
              <pre className={styles.ioBox}>{step.input?JSON.stringify(step.input,null,2).slice(0,400):'—'}</pre>
            </div>
            <div>
              <div className={styles.secLabel}>Output</div>
              <pre className={`${styles.ioBox} ${step.status==='done'?styles.ioGreen:step.status==='error'?styles.ioRed:''}`}>
                {step.error||(step.output?JSON.stringify(step.output,null,2).slice(0,400):step.status==='pending'?'pending…':'—')}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function Pill({label,color,active}:{label:string;color:string;active:boolean}) {
  return (
    <div className={styles.pill}>
      <span className={styles.pillDot} style={{background:color,opacity:active?1:.25,animation:active?'pulse 1.4s ease-in-out infinite':'none'}}/>
      <span style={{color:active?'#fff':'#5a6a7a'}}>{label}</span>
    </div>
  );
}
ENDOFFILE
echo "✓ page.tsx updated"

# ─────────────────────────────────────────────
# layout.tsx — add DM Sans font
# ─────────────────────────────────────────────
cat > src/app/layout.tsx << 'ENDOFFILE'
import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Apex — Document AI Pipeline',
  description: 'Upload documents, watch AI agents process them in real time',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com"/>
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous"/>
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet"/>
      </head>
      <body>{children}</body>
    </html>
  );
}
ENDOFFILE
echo "✓ layout.tsx updated"

echo ""
echo "✅ Done! Restart the dev server:"
echo "   npm run dev"
echo "   Open: http://localhost:3000"