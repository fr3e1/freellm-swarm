'use strict';

(function () {
  // ============================================================
  // Configuration & State
  // ============================================================
  const TOKEN = new URLSearchParams(window.location.search).get('token') || '';
  const API_BASE = '';
  const MAX_FEED_ITEMS = 300;
  const LIVE_TAIL_LINES = 30;
  const LIVE_FETCH_INTERVAL = 1000; // ms
  const STATS_REFRESH_INTERVAL = 3000; // ms
  const TPS_WINDOW_MS = 10000; // 10s sliding window
  const TICKER_INTERVAL = 1000; // 1s
  const EVENTSOURCE_RETRY_BASE = 1000;
  const EVENTSOURCE_MAX_RETRY = 30000;

  // State
  let runs = []; // RunSummary[]
  let selectedRunId = null;
  let runData = null; // full run data with tasks & events
  let tasksById = {}; // taskId -> TaskState (mutable, updated incrementally)
  let eventFeed = []; // array of event objects (newest first for display)
  let liveFetchTimers = {}; // key -> timer id (key = `${runId}:${taskId}`)
  let liveFetchPromises = {}; // key -> promise (to deduplicate)
  let progressCharHistory = []; // {ts, deltaChars} for TPS calculation
  let lastCharsPerTask = {}; // taskId -> last known chars (for delta calculation)
  let eventSource = null;
  let eventSourceRetryDelay = EVENTSOURCE_RETRY_BASE;
  let statsRefreshTimer = null;
  let tickerTimer = null;
  let detailUpdateTimer = null;
  let currentDetailTaskId = null;
  let currentDetailTab = 'live';
  let wasViewingNewestRun = true;
  let selectGeneration = 0; // monotonically increasing selection counter

  // DOM references
  const els = {};

  // ============================================================
  // Utility Functions
  // ============================================================
  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  function formatNumber(n) {
    return Number(n || 0).toLocaleString();
  }

  function formatDuration(seconds) {
    if (!seconds || seconds < 0) return '0s';
    const s = Math.floor(seconds);
    const m = Math.floor(s / 60);
    const h = Math.floor(m / 60);
    if (h > 0) return `${h}m ${String(m % 60).padStart(2, '0')}s`;
    if (m > 0) return `${m}m ${String(s % 60).padStart(2, '0')}s`;
    return `${s}s`;
  }

  function formatRelativeTime(ts) {
    if (!ts) return '';
    const diff = Date.now() / 1000 - ts;
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    const d = new Date(ts * 1000);
    return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  }

  function fetchWithToken(url, options = {}) {
    return fetch(url, {
      ...options,
      headers: {
        'Accept': 'application/json',
        ...(TOKEN ? { 'Authorization': `Bearer ${TOKEN}` } : {}),
        ...(options.headers || {})
      }
    });
  }

  function safeLocalStorageGet(key) {
    try {
      return localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function safeLocalStorageSet(key, value) {
    try {
      localStorage.setItem(key, value);
    } catch (e) {
      // ignore
    }
  }

  function createElement(tag, className, textContent) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (textContent !== undefined) el.textContent = textContent;
    return el;
  }

  function clearElement(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
  }

  // ============================================================
  // API Calls
  // ============================================================
  async function apiGetRuns() {
    const res = await fetchWithToken(`${API_BASE}/api/runs`);
    if (!res.ok) throw new Error(`Failed to fetch runs: ${res.status}`);
    return res.json();
  }

  async function apiGetRun(runId) {
    const res = await fetchWithToken(`${API_BASE}/api/runs/${encodeURIComponent(runId)}`);
    if (!res.ok) throw new Error(`Failed to fetch run: ${res.status}`);
    return res.json();
  }

  async function apiGetStats() {
    const res = await fetchWithToken(`${API_BASE}/api/stats`);
    if (!res.ok) throw new Error(`Failed to fetch stats: ${res.status}`);
    return res.json();
  }

  async function apiGetLive(runId, taskId, tail = LIVE_TAIL_LINES) {
    const res = await fetchWithToken(`${API_BASE}/api/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/live?tail=${tail}`);
    if (!res.ok) return '';
    return res.text();
  }

  async function apiGetFiles(runId, taskId) {
    const res = await fetchWithToken(`${API_BASE}/api/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/files`);
    if (!res.ok) return { files: [] };
    return res.json();
  }

  async function apiGetFile(runId, taskId, path) {
    const res = await fetchWithToken(`${API_BASE}/api/runs/${encodeURIComponent(runId)}/tasks/${encodeURIComponent(taskId)}/file?path=${encodeURIComponent(path)}`);
    if (!res.ok) return null;
    return res.text();
  }

  // ============================================================
  // Task State Replay (for incremental SSE updates only)
  // ============================================================
  function applyEventToTask(task, event) {
    const type = event.type;
    const ts = event.ts;

    switch (type) {
      case 'task_queued':
        task.status = 'queued';
        task.title = event.title || task.title;
        break;

      case 'task_start':
        if (task.status !== 'revising') {
          task.status = 'running';
        }
        if (!task.started) task.started = ts;
        // Reset char tracking for this task
        lastCharsPerTask[task.id] = 0;
        break;

      case 'progress':
        if (event.chars !== undefined) {
          task.chars = event.chars;
          if (event.model) task.model = event.model;
        }
        break;

      case 'attempt':
        task.attempts.push({ model: event.model, error: null });
        task.model = event.model;
        break;

      case 'attempt_error':
        for (let i = task.attempts.length - 1; i >= 0; i--) {
          if (task.attempts[i].model === event.model && task.attempts[i].error === null) {
            task.attempts[i].error = event.error;
            break;
          }
        }
        break;

      case 'task_done':
        task.status = 'done';
        task.finished = ts;
        task.model = event.model || task.model;
        task.seconds = event.seconds || 0;
        task.revision = event.revision || task.revision;
        task.files = event.files || [];
        task.notes = event.notes || '';
        task.errors = event.errors || [];
        const usage = event.usage || {};
        task.prompt_tokens += usage.prompt_tokens || 0;
        task.completion_tokens += usage.completion_tokens || 0;
        task.tokens += usage.total_tokens || (usage.prompt_tokens || 0) + (usage.completion_tokens || 0);
        break;

      case 'task_failed':
        task.status = 'failed';
        task.finished = ts;
        task.errors = event.errors || [];
        if (event.model) task.model = event.model;
        if (event.seconds !== undefined) task.seconds = event.seconds;
        if (event.revision !== undefined) task.revision = event.revision;
        if (event.files) task.files = event.files;
        if (event.notes) task.notes = event.notes;
        const usageF = event.usage || {};
        task.prompt_tokens += usageF.prompt_tokens || 0;
        task.completion_tokens += usageF.completion_tokens || 0;
        task.tokens += usageF.total_tokens || (usageF.prompt_tokens || 0) + (usageF.completion_tokens || 0);
        break;

      case 'revise_start':
        task.status = 'revising';
        task.chars = 0;
        task.revision = event.revision || task.revision + 1;
        lastCharsPerTask[task.id] = 0;
        if (event.feedback) {
          task.feedback.push({ ts, text: event.feedback, revision: task.revision });
        }
        break;

      case 'verdict':
        task.verdict = event.verdict;
        task.verdict_text = event.text || '';
        break;

      case 'apply':
        task.applied = true;
        if (event.files) task.files = event.files;
        break;

      case 'supervisor_note':
        if (event.text) {
          task.supervisor_notes.push({ ts, text: event.text });
        }
        break;
    }
  }

  // ============================================================
  // Rendering - Sidebar (Runs List)
  // ============================================================
  function renderSidebar() {
    const list = els.runsList;
    clearElement(list);

    for (const run of runs) {
      const li = createElement('li', 'run-item');
      li.dataset.run = run.id;
      if (run.id === selectedRunId) li.classList.add('active');

      const nameDiv = createElement('div', 'run-name', run.name || run.id);
      const metaDiv = createElement('div', 'run-meta');
      
      if (run.started == null) {
        metaDiv.textContent = 'no events';
      } else {
        const started = new Date(run.started * 1000).toLocaleTimeString();
        const ended = run.ended ? new Date(run.ended * 1000).toLocaleTimeString() : 'running';
        metaDiv.textContent = `${started} - ${ended} • ${run.task_count} tasks • ${formatNumber(run.tokens)} tokens`;
      }

      li.appendChild(nameDiv);
      li.appendChild(metaDiv);
      li.addEventListener('click', () => selectRun(run.id));
      list.appendChild(li);
    }
  }

  function updateRunInSidebar(runSummary) {
    const idx = runs.findIndex(r => r.id === runSummary.id);
    if (idx >= 0) {
      runs[idx] = runSummary;
    } else {
      runs.unshift(runSummary);
    }
    runs.sort((a, b) => (b.started || 0) - (a.started || 0));
    renderSidebar();
  }

  // ============================================================
  // Rendering - Task Grid
  // ============================================================
  function renderTaskGrid() {
    const grid = els.taskGrid;
    clearElement(grid);

    if (!runData || !runData.tasks) return;

    // Sort tasks: running/revising first, then by order in runData.tasks
    const taskOrder = runData.tasks.map(t => t.id);
    const sortedTasks = Object.values(tasksById).sort((a, b) => {
      const aRunning = a.status === 'running' || a.status === 'revising';
      const bRunning = b.status === 'running' || b.status === 'revising';
      if (aRunning !== bRunning) return bRunning - aRunning;
      return taskOrder.indexOf(a.id) - taskOrder.indexOf(b.id);
    });

    for (const task of sortedTasks) {
      grid.appendChild(createTaskCard(task));
    }

    // Initial live fetch for tasks with output
    for (const task of Object.values(tasksById)) {
      if (task.chars > 0 || task.status === 'done' || task.status === 'failed') {
        fetchAndUpdateLive(selectedRunId, task.id);
      }
      if (task.status === 'running' || task.status === 'revising') {
        scheduleLiveFetch(selectedRunId, task.id);
      }
    }
  }

  function createTaskCard(task) {
    const article = createElement('article', `task-card status-${task.status}`);
    article.dataset.task = task.id;

    // Task head
    const head = createElement('div', 'task-head');
    const title = createElement('div', 'task-title', task.title);
    const statusBadge = createElement('span', `task-status status-${task.status}`, getStatusLabel(task.status));
    head.appendChild(title);
    head.appendChild(statusBadge);
    article.appendChild(head);

    // Model
    if (task.model) {
      const modelDiv = createElement('div', 'task-model', `Model: ${task.model}`);
      article.appendChild(modelDiv);
    }

    // Metrics - single inline row
    const metrics = createElement('div', 'task-metrics');
    metrics.textContent = formatTaskMetrics(task);
    article.appendChild(metrics);

    // Live output
    const livePre = createElement('pre', 'task-live');
    livePre.textContent = '';
    article.appendChild(livePre);

    // Verdict
    if (task.verdict) {
      const verdictDiv = createElement('div', `task-verdict verdict-${task.verdict}`);
      verdictDiv.textContent = `${task.verdict}: ${task.verdict_text}`;
      article.appendChild(verdictDiv);
    } else {
      const verdictDiv = createElement('div', 'task-verdict');
      article.appendChild(verdictDiv);
    }

    // Attempts
    const attemptsDiv = createElement('div', 'task-attempts');
    if (task.attempts.length > 0) {
      const list = createElement('ul');
      for (const attempt of task.attempts) {
        const li = createElement('li');
        li.textContent = `${attempt.model}${attempt.error ? ` - ${attempt.error}` : ''}`;
        if (attempt.error) li.classList.add('attempt-error');
        list.appendChild(li);
      }
      attemptsDiv.appendChild(list);
    }
    article.appendChild(attemptsDiv);

    // Click to open detail
    article.addEventListener('click', (e) => {
      if (e.target.closest('.task-live')) return;
      openTaskDetail(task.id);
    });

    return article;
  }

  function formatTaskMetrics(task) {
    let elapsed;
    if (task.status === 'done' || task.status === 'failed') {
      elapsed = task.seconds || 0;
    } else if (task.started) {
      elapsed = (task.finished || Date.now() / 1000) - task.started;
    } else {
      elapsed = 0;
    }
    return `⏱ ${formatDuration(elapsed)} · ${formatNumber(task.chars)} chars · ${formatNumber(task.tokens)} tok · rev ${task.revision}`;
  }

  function getStatusLabel(status) {
    const labels = {
      queued: 'Queued',
      running: 'Running',
      revising: 'Revising',
      done: 'Done',
      failed: 'Failed'
    };
    return labels[status] || status;
  }

  function updateTaskCard(task) {
    const card = els.taskGrid.querySelector(`.task-card[data-task="${task.id}"]`);
    if (!card) return;

    card.className = `task-card status-${task.status}`;

    const badge = card.querySelector('.task-status');
    if (badge) {
      badge.className = `task-status status-${task.status}`;
      badge.textContent = getStatusLabel(task.status);
    }

    let modelDiv = card.querySelector('.task-model');
    if (task.model) {
      if (!modelDiv) {
        modelDiv = createElement('div', 'task-model');
        card.insertBefore(modelDiv, card.querySelector('.task-metrics'));
      }
      modelDiv.textContent = `Model: ${task.model}`;
    } else if (modelDiv) {
      modelDiv.remove();
    }

    const metrics = card.querySelector('.task-metrics');
    if (metrics) {
      metrics.textContent = formatTaskMetrics(task);
    }

    let verdictDiv = card.querySelector('.task-verdict');
    if (task.verdict) {
      if (!verdictDiv) {
        verdictDiv = createElement('div', 'task-verdict');
        card.insertBefore(verdictDiv, card.querySelector('.task-attempts'));
      }
      verdictDiv.className = `task-verdict verdict-${task.verdict}`;
      verdictDiv.textContent = `${task.verdict}: ${task.verdict_text}`;
    } else if (verdictDiv) {
      verdictDiv.textContent = '';
      verdictDiv.className = 'task-verdict';
    }

    const attemptsDiv = card.querySelector('.task-attempts');
    if (attemptsDiv) {
      clearElement(attemptsDiv);
      if (task.attempts.length > 0) {
        const list = createElement('ul');
        for (const attempt of task.attempts) {
          const li = createElement('li');
          li.textContent = `${attempt.model}${attempt.error ? ` - ${attempt.error}` : ''}`;
          if (attempt.error) li.classList.add('attempt-error');
          list.appendChild(li);
        }
        attemptsDiv.appendChild(list);
      }
    }
  }

  // ============================================================
  // Rendering - Event Feed
  // ============================================================
  function renderEventFeed() {
    const feed = els.eventFeed;
    clearElement(feed);

    for (const event of eventFeed) {
      feed.appendChild(createFeedItem(event));
    }
  }

  function prependEventToFeed(event) {
    // Skip progress events in feed
    if (event.type === 'progress') return;

    eventFeed.unshift(event);
    if (eventFeed.length > MAX_FEED_ITEMS) eventFeed.pop();

    const feed = els.eventFeed;
    const item = createFeedItem(event);
    feed.insertBefore(item, feed.firstChild);

    while (feed.children.length > MAX_FEED_ITEMS) {
      feed.removeChild(feed.lastChild);
    }
  }

  function createFeedItem(event) {
    const li = createElement('li', `ev ev-${event.type}`);
    if (['supervisor_note', 'verdict', 'revise_start'].includes(event.type)) {
      li.classList.add('ev-supervisor');
    }

    const time = createElement('span', 'ev-time', formatRelativeTime(event.ts));
    const type = createElement('span', 'ev-type', event.type);
    const task = event.task ? createElement('span', 'ev-task', `#${event.task}`) : null;
    const detail = createElement('span', 'ev-detail', formatEventDetail(event));

    li.appendChild(time);
    li.appendChild(type);
    if (task) li.appendChild(task);
    li.appendChild(detail);

    return li;
  }

  function formatEventDetail(event) {
    switch (event.type) {
      case 'run_start':
        return `Run started: ${event.name || ''} (${event.tasks?.length || 0} tasks)`;
      case 'run_end':
        return 'Run ended';
      case 'task_queued':
        return `Queued: ${event.title || ''}`;
      case 'task_start':
        return `Started: ${event.title || ''}`;
      case 'attempt':
        return `Attempt: ${event.model || ''}`;
      case 'attempt_error':
        return `Error (${event.model || ''}): ${event.error || ''}`;
      case 'task_done':
        return `Done in ${event.seconds ? formatDuration(event.seconds) : '?'} • ${formatNumber(event.usage?.total_tokens || 0)} tokens`;
      case 'task_failed':
        return `Failed: ${event.errors?.join(', ') || 'Unknown error'}`;
      case 'revise_start':
        return `Revision ${event.revision || ''}: ${event.feedback || ''}`;
      case 'supervisor_note':
        return event.text || '';
      case 'verdict':
        return `${event.verdict || ''}: ${event.text || ''}`;
      case 'apply':
        return `Applied ${event.files?.length || 0} files`;
      default:
        return JSON.stringify(event).slice(0, 100);
    }
  }

  // ============================================================
  // Rendering - Stats Header
  // ============================================================
  function renderStats(stats) {
    els.statTokens.textContent = formatNumber(stats.total_tokens);
    els.statActive.textContent = formatNumber(stats.active_tasks);
    els.statDone.textContent = formatNumber(stats.tasks_done);
    els.statFailed.textContent = formatNumber(stats.tasks_failed);
  }

  function updateTPS() {
    const now = Date.now();
    const cutoff = now - TPS_WINDOW_MS;
    const recent = progressCharHistory.filter(h => h.ts > cutoff);
    progressCharHistory = recent;

    if (recent.length === 0) {
      els.statTps.textContent = '0';
      return;
    }

    const totalDelta = recent.reduce((sum, h) => sum + h.deltaChars, 0);
    const timeSpan = (recent[recent.length - 1].ts - recent[0].ts) / 1000 || 1;
    const tps = Math.round(totalDelta / 4 / timeSpan);
    els.statTps.textContent = formatNumber(Math.max(0, tps));
  }

  // ============================================================
  // Live Output Fetching
  // ============================================================
  function scheduleLiveFetch(runId, taskId) {
    const key = `${runId}:${taskId}`;
    if (liveFetchTimers[key]) return;

    liveFetchTimers[key] = setTimeout(async () => {
      delete liveFetchTimers[key];
      await fetchAndUpdateLive(runId, taskId);
      const task = tasksById[taskId];
      if (task && (task.status === 'running' || task.status === 'revising')) {
        scheduleLiveFetch(runId, taskId);
      }
    }, LIVE_FETCH_INTERVAL);
  }

  async function fetchAndUpdateLive(runId, taskId) {
    const key = `${runId}:${taskId}`;
    if (liveFetchPromises[key]) return liveFetchPromises[key];

    liveFetchPromises[key] = (async () => {
      try {
        const text = await apiGetLive(runId, taskId, LIVE_TAIL_LINES);
        updateLiveDisplay(taskId, text);
      } catch (e) {
        console.warn('Live fetch failed:', e);
      } finally {
        delete liveFetchPromises[key];
      }
    })();

    return liveFetchPromises[key];
  }

  function updateLiveDisplay(taskId, text) {
    const card = els.taskGrid.querySelector(`.task-card[data-task="${taskId}"]`);
    if (card) {
      const livePre = card.querySelector('.task-live');
      if (livePre) {
        const wasScrolledToBottom = livePre.scrollTop + livePre.clientHeight >= livePre.scrollHeight - 10;
        livePre.textContent = text;
        if (wasScrolledToBottom) {
          livePre.scrollTop = livePre.scrollHeight;
        }
      }
    }

    if (currentDetailTaskId === taskId && currentDetailTab === 'live') {
      const detailLive = document.getElementById('detail-live-content');
      if (detailLive) {
        const wasScrolledToBottom = detailLive.scrollTop + detailLive.clientHeight >= detailLive.scrollHeight - 10;
        detailLive.textContent = text;
        if (wasScrolledToBottom) {
          detailLive.scrollTop = detailLive.scrollHeight;
        }
      }
    }
  }

  // ============================================================
  // Task Detail Dialog
  // ============================================================
  function openTaskDetail(taskId) {
    const task = tasksById[taskId];
    if (!task) return;

    currentDetailTaskId = taskId;
    currentDetailTab = 'live';

    els.detailTitle.textContent = task.title;
    els.taskDetail.showModal();

    for (const btn of els.detailTabs.querySelectorAll('button')) {
      btn.classList.toggle('active', btn.dataset.tab === 'live');
    }

    renderDetailTab(task, 'live');
    startDetailLiveUpdates(taskId);
  }

  function closeTaskDetail() {
    els.taskDetail.close();
    stopDetailLiveUpdates();
    currentDetailTaskId = null;
  }

  function startDetailLiveUpdates(taskId) {
    stopDetailLiveUpdates();
    detailUpdateTimer = setInterval(() => {
      if (currentDetailTaskId === taskId && currentDetailTab === 'live') {
        fetchAndUpdateLive(selectedRunId, taskId);
      }
    }, LIVE_FETCH_INTERVAL);
  }

  function stopDetailLiveUpdates() {
    if (detailUpdateTimer) {
      clearInterval(detailUpdateTimer);
      detailUpdateTimer = null;
    }
  }

  async function renderDetailTab(task, tab) {
    const body = els.detailBody;
    clearElement(body);

    switch (tab) {
      case 'live':
        body.appendChild(createDetailLiveTab(task));
        break;
      case 'files':
        body.appendChild(await createDetailFilesTab(task));
        break;
      case 'review':
        body.appendChild(createDetailReviewTab(task));
        break;
      case 'attempts':
        body.appendChild(createDetailAttemptsTab(task));
        break;
    }
  }

  function createDetailLiveTab(task) {
    const container = createElement('div', 'detail-tab-content');
    const pre = createElement('pre', 'detail-live');
    pre.id = 'detail-live-content';
    pre.textContent = 'Loading...';
    container.appendChild(pre);
    fetchAndUpdateLive(selectedRunId, task.id);
    return container;
  }

  async function createDetailFilesTab(task) {
    const container = createElement('div', 'detail-tab-content');
    const list = createElement('ul', 'detail-files-list');

    try {
      const { files } = await apiGetFiles(selectedRunId, task.id);
      for (const file of files.sort()) {
        const li = createElement('li');
        const link = createElement('a', 'file-link', file);
        link.href = '#';
        link.dataset.path = file;
        link.addEventListener('click', (e) => {
          e.preventDefault();
          viewFile(task.id, file);
        });
        li.appendChild(link);
        list.appendChild(li);
      }
      if (files.length === 0) {
        const li = createElement('li', 'empty', 'No files');
        list.appendChild(li);
      }
    } catch (e) {
      const li = createElement('li', 'error', 'Failed to load files');
      list.appendChild(li);
    }

    container.appendChild(list);
    return container;
  }

  async function viewFile(taskId, path) {
    try {
      const content = await apiGetFile(selectedRunId, taskId, path);
      if (content === null) {
        alert('File not found or too large');
        return;
      }
      const win = window.open('', '_blank');
      win.document.write(`<pre>${escapeHtml(content)}</pre>`);
      win.document.title = path;
      win.document.close();
    } catch (e) {
      alert('Failed to load file');
    }
  }

  function createDetailReviewTab(task) {
    const container = createElement('div', 'detail-tab-content');

    if (task.notes) {
      const section = createElement('div', 'review-section');
      section.appendChild(createElement('h4', '', 'Notes'));
      const pre = createElement('pre', 'review-notes');
      pre.textContent = task.notes;
      section.appendChild(pre);
      container.appendChild(section);
    }

    if (task.feedback.length > 0) {
      const section = createElement('div', 'review-section');
      section.appendChild(createElement('h4', '', 'Feedback History'));
      const list = createElement('ul', 'review-feedback');
      for (const fb of task.feedback) {
        const li = createElement('li');
        const strong = createElement('strong');
        strong.textContent = `Rev ${fb.revision} (${formatRelativeTime(fb.ts)}):`;
        li.appendChild(strong);
        li.appendChild(document.createTextNode(' '));
        li.appendChild(document.createTextNode(fb.text));
        list.appendChild(li);
      }
      section.appendChild(list);
      container.appendChild(section);
    }

    if (task.supervisor_notes.length > 0) {
      const section = createElement('div', 'review-section');
      section.appendChild(createElement('h4', '', 'Supervisor Notes'));
      const list = createElement('ul', 'review-supervisor');
      for (const note of task.supervisor_notes) {
        const li = createElement('li');
        const timeSpan = createElement('span', 'note-time', formatRelativeTime(note.ts));
        li.appendChild(timeSpan);
        li.appendChild(document.createTextNode(' '));
        li.appendChild(document.createTextNode(note.text));
        list.appendChild(li);
      }
      section.appendChild(list);
      container.appendChild(section);
    }

    if (task.verdict) {
      const section = createElement('div', 'review-section');
      section.appendChild(createElement('h4', '', 'Verdict'));
      const verdictDiv = createElement('div', `verdict-chip verdict-${task.verdict}`);
      verdictDiv.textContent = `${task.verdict}: ${task.verdict_text}`;
      section.appendChild(verdictDiv);
      container.appendChild(section);
    }

    if (container.children.length === 0) {
      container.appendChild(createElement('div', 'empty', 'No review data'));
    }

    return container;
  }

  function createDetailAttemptsTab(task) {
    const container = createElement('div', 'detail-tab-content');

    if (task.attempts.length === 0) {
      container.appendChild(createElement('div', 'empty', 'No attempts'));
      return container;
    }

    const list = createElement('ul', 'attempts-list');
    for (const attempt of task.attempts) {
      const li = createElement('li', attempt.error ? 'attempt-error' : 'attempt-success');
      li.textContent = `${attempt.model}${attempt.error ? ` - ${attempt.error}` : ' ✓'}`;
      list.appendChild(li);
    }
    container.appendChild(list);
    return container;
  }

  // ============================================================
  // EventSource Handling
  // ============================================================
  function connectEventSource() {
    if (eventSource) {
      eventSource.close();
    }

    let url = `${API_BASE}/api/events`;
    if (TOKEN) url += `?token=${encodeURIComponent(TOKEN)}`;

    eventSource = new EventSource(url);

    eventSource.onopen = () => {
      els.connStatus.textContent = 'Connected';
      els.connStatus.className = 'conn conn-ok';
      eventSourceRetryDelay = EVENTSOURCE_RETRY_BASE;
      // Reconcile missed events on reconnect
      if (selectedRunId) {
        reconcileOnReconnect();
      }
    };

    eventSource.onerror = () => {
      els.connStatus.textContent = 'Disconnected';
      els.connStatus.className = 'conn conn-down';
      eventSource.close();
      setTimeout(connectEventSource, eventSourceRetryDelay);
      eventSourceRetryDelay = Math.min(eventSourceRetryDelay * 2, EVENTSOURCE_MAX_RETRY);
    };

    eventSource.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data);
        handleServerEvent(event);
      } catch (err) {
        console.warn('Failed to parse event:', err);
      }
    };
  }

  async function reconcileOnReconnect() {
    try {
      // Refetch runs, stats, and the selected run to reconcile missed events
      const [runsData, stats] = await Promise.all([apiGetRuns(), apiGetStats()]);
      runs = runsData.runs;
      renderStats(stats);
      renderSidebar();
      if (selectedRunId) {
        await selectRun(selectedRunId);
      }
    } catch (e) {
      console.warn('Reconciliation failed:', e);
    }
  }

  function handleServerEvent(event) {
    // Handle progress events for TPS and live updates (but not feed)
    if (event.type === 'progress' && event.chars !== undefined && event.task) {
      const taskId = event.task;
      const lastChars = lastCharsPerTask[taskId] || 0;
      const delta = event.chars - lastChars;
      if (delta > 0) {
        progressCharHistory.push({ ts: Date.now(), deltaChars: delta });
        updateTPS();
      }
      lastCharsPerTask[taskId] = event.chars;

      // Update task chars in memory
      if (tasksById[taskId]) {
        tasksById[taskId].chars = event.chars;
        if (event.model) tasksById[taskId].model = event.model;
        updateTaskCard(tasksById[taskId]);
      }

      // Schedule live fetch for running/revising tasks
      if (tasksById[taskId] && (tasksById[taskId].status === 'running' || tasksById[taskId].status === 'revising')) {
        scheduleLiveFetch(selectedRunId, taskId);
      }
    }

    // If event belongs to selected run, apply incrementally
    if (event.run === selectedRunId && event.task) {
      if (!tasksById[event.task]) {
        tasksById[event.task] = createInitialTaskState(event.task, event.title || 'Unknown');
      }
      applyEventToTask(tasksById[event.task], event);
      updateTaskCard(tasksById[event.task]);

      // Refresh stats on task_done/task_failed
      if (event.type === 'task_done' || event.type === 'task_failed') {
        refreshStats();
      }
    }

    // Prepend to feed (skips progress events internally)
    prependEventToFeed(event);

    // Handle new run
    if (event.type === 'run_start') {
      const newRun = {
        id: event.run,
        name: event.name,
        started: event.ts,
        ended: null,
        task_count: event.tasks?.length || 0,
        counts: { queued: event.tasks?.length || 0, running: 0, revising: 0, done: 0, failed: 0 },
        tokens: 0
      };
      updateRunInSidebar(newRun);

      if (wasViewingNewestRun && runs.length > 0 && runs[0].id === event.run) {
        selectRun(event.run);
      }
    }

    // Handle run end
    if (event.type === 'run_end') {
      const run = runs.find(r => r.id === event.run);
      if (run) run.ended = event.ts;
      if (selectedRunId === event.run) {
        const ended = event.ts ? new Date(event.ts * 1000).toLocaleString() : '?';
        els.runMeta.textContent = els.runMeta.textContent.replace(/ • running$/, ` • ${ended}`);
      }
      renderSidebar();
    }

    scheduleSidebarRefresh();
  }

  let sidebarRefreshTimer = null;
  function scheduleSidebarRefresh() {
    if (sidebarRefreshTimer) return;
    sidebarRefreshTimer = setTimeout(async () => {
      sidebarRefreshTimer = null;
      try {
        const data = await apiGetRuns();
        runs = data.runs;
        renderSidebar();
        wasViewingNewestRun = selectedRunId === runs[0]?.id;
      } catch (e) {
        console.warn('Sidebar refresh failed:', e);
      }
    }, STATS_REFRESH_INTERVAL);
  }

  async function refreshStats() {
    try {
      const stats = await apiGetStats();
      renderStats(stats);
    } catch (e) {
      console.warn('Stats refresh failed:', e);
    }
  }

  // ============================================================
  // Run Selection
  // ============================================================
  function createInitialTaskState(taskId, title) {
    return {
      id: taskId,
      title,
      status: 'queued',
      model: null,
      attempts: [],
      chars: 0,
      seconds: 0,
      prompt_tokens: 0,
      completion_tokens: 0,
      tokens: 0,
      revision: 0,
      files: [],
      notes: '',
      errors: [],
      verdict: null,
      verdict_text: '',
      applied: false,
      supervisor_notes: [],
      feedback: [],
      started: null,
      finished: null
    };
  }

  async function selectRun(runId) {
    // Close detail dialog and stop its polling
    closeTaskDetail();

    // Clear all live fetch timers and promises
    liveFetchTimers = {};
    liveFetchPromises = {};

    // Reset char tracking baselines and TPS history
    lastCharsPerTask = {};
    progressCharHistory = [];

    const generation = ++selectGeneration;
    selectedRunId = runId;
    safeLocalStorageSet('swarm.run', runId);
    wasViewingNewestRun = runs.length > 0 && runs[0].id === runId;

    for (const li of els.runsList.querySelectorAll('.run-item')) {
      li.classList.toggle('active', li.dataset.run === runId);
    }

    try {
      runData = await apiGetRun(runId);
      if (generation !== selectGeneration) return; // stale selection, ignore

      // Use server's authoritative TaskState from runData.tasks directly
      tasksById = {};
      if (runData.tasks) {
        for (const task of runData.tasks) {
          tasksById[task.id] = { ...task };
          // Ensure arrays exist
          tasksById[task.id].attempts = task.attempts || [];
          tasksById[task.id].files = task.files || [];
          tasksById[task.id].errors = task.errors || [];
          tasksById[task.id].supervisor_notes = task.supervisor_notes || [];
          tasksById[task.id].feedback = task.feedback || [];
          // Initialize lastCharsPerTask for running/revising tasks
          if (task.status === 'running' || task.status === 'revising') {
            lastCharsPerTask[task.id] = task.chars || 0;
          }
        }
      }
      
      // Event feed from runData.events (last 300, oldest first) - reverse for newest first
      eventFeed = (runData.events || []).slice(-MAX_FEED_ITEMS).reverse();

      els.runTitle.textContent = runData.name || runId;
      const started = runData.started ? new Date(runData.started * 1000).toLocaleString() : '?';
      const ended = runData.ended ? new Date(runData.ended * 1000).toLocaleString() : 'running';
      els.runMeta.textContent = `Started: ${started} • ${ended} • ${runData.tasks?.length || 0} tasks`;

      renderTaskGrid();
      renderEventFeed();

    } catch (e) {
      console.error('Failed to load run:', e);
      els.runTitle.textContent = 'Error loading run';
      els.runMeta.textContent = '';
      clearElement(els.taskGrid);
      clearElement(els.eventFeed);
    }
  }

  // ============================================================
  // Ticker (1s elapsed time updates)
  // ============================================================
  function startTicker() {
    tickerTimer = setInterval(() => {
      for (const task of Object.values(tasksById)) {
        if (task.status === 'running' || task.status === 'revising') {
          const metrics = els.taskGrid.querySelector(`.task-card[data-task="${task.id}"] .task-metrics`);
          if (metrics && task.started) {
            const elapsed = (Date.now() / 1000) - task.started;
            metrics.textContent = formatTaskMetrics({ ...task, seconds: elapsed });
          }
        }
      }
    }, TICKER_INTERVAL);
  }

  // ============================================================
  // Initialization
  // ============================================================
  function cacheDOM() {
    els.statTokens = document.getElementById('stat-tokens');
    els.statTps = document.getElementById('stat-tps');
    els.statActive = document.getElementById('stat-active');
    els.statDone = document.getElementById('stat-done');
    els.statFailed = document.getElementById('stat-failed');
    els.connStatus = document.getElementById('conn-status');
    els.runsList = document.getElementById('runs-list');
    els.runTitle = document.getElementById('run-title');
    els.runMeta = document.getElementById('run-meta');
    els.taskGrid = document.getElementById('task-grid');
    els.eventFeed = document.getElementById('event-feed');
    els.taskDetail = document.getElementById('task-detail');
    els.detailTitle = document.getElementById('detail-title');
    els.detailTabs = document.getElementById('detail-tabs');
    els.detailBody = document.getElementById('detail-body');
    els.detailClose = document.getElementById('detail-close');
  }

  function bindEvents() {
    els.detailTabs.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-tab]');
      if (!btn) return;
      currentDetailTab = btn.dataset.tab;
      for (const b of els.detailTabs.querySelectorAll('button')) {
        b.classList.toggle('active', b === btn);
      }
      if (currentDetailTaskId) {
        renderDetailTab(tasksById[currentDetailTaskId], currentDetailTab);
        if (currentDetailTab === 'live') {
          startDetailLiveUpdates(currentDetailTaskId);
        } else {
          stopDetailLiveUpdates();
        }
      }
    });

    els.detailClose.addEventListener('click', closeTaskDetail);
    els.taskDetail.addEventListener('click', (e) => {
      if (e.target === els.taskDetail) closeTaskDetail();
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        if (els.taskDetail.open) closeTaskDetail();
      }
    });
  }

  async function init() {
    cacheDOM();
    bindEvents();

    try {
      const [runsData, stats] = await Promise.all([apiGetRuns(), apiGetStats()]);
      runs = runsData.runs;
      renderStats(stats);
      renderSidebar();

      const savedRun = safeLocalStorageGet('swarm.run');
      let runToSelect = null;
      if (savedRun && runs.some(r => r.id === savedRun)) {
        runToSelect = savedRun;
        wasViewingNewestRun = false;
      } else if (runs.length > 0) {
        runToSelect = runs[0].id;
        wasViewingNewestRun = true;
      }

      if (runToSelect) {
        await selectRun(runToSelect);
      }
    } catch (e) {
      console.error('Initial load failed:', e);
      els.runTitle.textContent = 'Failed to load dashboard';
    }

    connectEventSource();
    startTicker();
    statsRefreshTimer = setInterval(refreshStats, 10000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
