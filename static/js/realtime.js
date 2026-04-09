// static/js/realtime.js
// ================================================================
// RealtimeManager — handles all auto-refresh for DocPro
// Works for both Admin and Resource Person dashboards.
// Uses polling as primary method.
// ================================================================

class RealtimeManager {

  constructor(options = {}) {
    this.role        = options.role || 'resource';  // 'admin' | 'resource'
    this.intervals   = {};
    this.handlers    = {};
    this.isRunning   = false;
    this.csrfToken   = this.getCsrf();
    this.retryCount  = {};

    // Polling intervals (ms)
    this.POLL_INTERVALS = {
      heartbeat:        15000,  // 15s  — keep alive
      dashboard:        5000,   // 5s   — admin summary
      resources:        10000,  // 10s  — online/offline status
      documents:        5000,   // 5s   — upload + pipeline
      queue:            4000,   // 4s   — assignment queue
      submitted:        5000,   // 5s   — review queue
    };

    this.MAX_RETRY = 5;
    
    // Inject Styles if not already present
    this._injectStyles();
  }

  // ── Start all pollers ─────────────────────────────────────

  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    if (this.role === 'resource') {
      this._startResourcePollers();
    } else {
      this._startAdminPollers();
    }

    console.log(`[Realtime] Started (role=${this.role})`);
  }

  stop() {
    Object.values(this.intervals).forEach(clearInterval);
    this.intervals = {};
    this.isRunning = false;
    console.log('[Realtime] Stopped');
  }

  // ── Register UI update handler ────────────────────────────

  on(event, handler) {
    this.handlers[event] = handler;
    return this;
  }

  emit(event, data) {
    if (this.handlers[event]) {
      try {
        this.handlers[event](data);
      } catch (e) {
        console.error(`[Realtime] Handler error for ${event}:`, e);
      }
    }
  }

  // ── Admin pollers ─────────────────────────────────────────

  _startAdminPollers() {
    // Single combined dashboard poll (most efficient)
    this._poll(
      'dashboard',
      '/api/v1/processing/admin/dashboard-summary/',
      (data) => {
        this.emit('dashboard_summary', data);
        this._updateAdminSummaryBadges(data);
      }
    );

    // Resource online/offline status
    this._poll(
      'resources',
      '/api/v1/processing/admin/resources/status/',
      (data) => {
        this.emit('resource_status_list', data);
        this._updateResourceStatusTable(data);
      }
    );

    // Document list + pipeline
    this._poll(
      'documents',
      '/api/v1/processing/admin/documents/refresh/',
      (data) => {
        this.emit('documents_updated', data);
        this._updateDocumentTable(data);
      }
    );

    // Assignment queue
    this._poll(
      'queue',
      '/api/v1/processing/queue/',
      (data) => {
        this.emit('queue_updated', data);
        this._updateQueueDisplay(data);
      }
    );

    // Submitted pages review queue
    this._poll(
      'submitted',
      '/api/v1/processing/admin/submitted-queue/',
      (data) => {
        this.emit('submitted_queue', data);
        this._updateSubmittedQueue(data);
      }
    );
  }

  // ── Resource pollers ──────────────────────────────────────

  _startResourcePollers() {
    // Heartbeat — keeps resource marked online
    this._heartbeat();
    this.intervals['heartbeat'] = setInterval(
      () => this._heartbeat(),
      this.POLL_INTERVALS.heartbeat
    );

    // Fetch assigned work
    this._poll(
      'queue',
      '/api/v1/processing/queue/',
      (data) => {
        this.emit('assignments_updated', data);
        this._updateAssignmentList(data);
      }
    );
  }

  // ── Core polling engine ───────────────────────────────────

  _poll(name, url, onSuccess) {
    const execute = async () => {
      try {
        const res  = await fetch(url, {
          headers: { 'Accept': 'application/json' },
          credentials: 'include'
        });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data = await res.json();
        this.retryCount[name] = 0;
        onSuccess(data);
      } catch (err) {
        this.retryCount[name] = (this.retryCount[name] || 0) + 1;
        console.warn(
          `[Realtime] Poll "${name}" failed `
          + `(attempt ${this.retryCount[name]}):`, err.message
        );
        if (this.retryCount[name] >= this.MAX_RETRY) {
          console.error(
            `[Realtime] Poll "${name}" suspended after `
            + `${this.MAX_RETRY} failures`
          );
          clearInterval(this.intervals[name]);
          this.emit('poll_suspended', { name });
        }
      }
    };

    // Run immediately then set interval
    execute();
    this.intervals[name] = setInterval(
      execute,
      this.POLL_INTERVALS[name]
    );
  }

  // ── Heartbeat ─────────────────────────────────────────────

  async _heartbeat() {
    try {
      this.csrfToken = this.getCsrf(); // Refresh on each cycle
      const res = await fetch('/api/v1/processing/heartbeat/', {
        method:  'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken':  this.csrfToken,
        },
        credentials: 'include',
        body: JSON.stringify({
          current_page: window.location.pathname,
        }),
      });
      const data = await res.json();
      this.emit('heartbeat_ack', data);
      this._updateOnlineIndicator(true);
    } catch (err) {
      console.warn('[Realtime] Heartbeat failed:', err.message);
      this._updateOnlineIndicator(false);
    }
  }

  // ── UI update methods — Admin ─────────────────────────────

  _updateAdminSummaryBadges(data) {
    // Resource counts
    this._setText('stat-online',   data.resources?.online  || 0);
    this._setText('stat-offline',  data.resources?.offline || 0);
    this._setText('stat-away',     data.resources?.away    || 0);

    // Queue counts
    this._setText('stat-unassigned',    data.queue?.unassigned_pages || 0);
    this._setText('stat-in-progress',   data.queue?.in_progress_pages || 0);
    this._setText('stat-pending-review', data.queue?.pending_review || 0);

    // Pipeline badge
    const converting = data.pipeline?.CONVERTING || 0;
    const convBadge  = document.getElementById('stat-converting');
    if (convBadge) {
      convBadge.textContent = converting;
      convBadge.classList.toggle('badge-active', converting > 0);
    }
  }

  _updateResourceStatusTable(data) {
    const table = document.getElementById('resource-status-table');
    if (!table) return;

    data.resources.forEach(r => {
      // Find existing row or create new one
      let row = table.querySelector(`[data-resource-id="${r.id}"]`);

      if (!row) {
        row = document.createElement('tr');
        row.setAttribute('data-resource-id', r.id);
        const tbody = table.querySelector('tbody');
        if (tbody) tbody.appendChild(row);
      }

      // Update status badge
      const statusClass = {
        online:  'status-online',
        away:    'status-away',
        offline: 'status-offline',
      }[r.online_status] || 'status-offline';

      // Update capacity bar
      const loadPct = r.max_capacity > 0
        ? Math.min((r.current_load / r.max_capacity) * 100, 100)
        : 0;

      row.innerHTML = `
        <td>${r.full_name || r.username}</td>
        <td>
          <span class="status-dot ${statusClass}"></span>
          ${r.online_status}
        </td>
        <td>
          <div class="capacity-bar-wrap">
            <div class="capacity-bar-fill"
                 style="width:${loadPct.toFixed(1)}%">
            </div>
          </div>
          <small>${r.current_load.toFixed(1)} / ${r.max_capacity}</small>
        </td>
        <td>${r.assigned_pages}</td>
        <td>${r.last_seen
          ? this._timeAgo(r.last_seen)
          : 'Never'}</td>
      `;
    });

    // Update online count header
    this._setText('online-count', data.online_count || 0);
  }

  _updateDocumentTable(data) {
    const table = document.getElementById('document-table');
    const tbody = table ? table.querySelector('tbody') : null;
    if (!tbody) return;

    data.documents.forEach(doc => {
      let row = tbody.querySelector(`[data-doc-ref="${doc.doc_ref}"]`);

      // New document — prepend row
      if (!row) {
        row = document.createElement('tr');
        row.setAttribute('data-doc-ref', doc.doc_ref);
        tbody.insertBefore(row, tbody.firstChild);
        // Flash new row
        row.classList.add('row-flash-new');
        setTimeout(() => row.classList.remove('row-flash-new'), 2000);
      }

      const statusClass = {
        CONVERTING:       'badge-warning',
        SPLITTING:        'badge-warning',
        READY_TO_ASSIGN:  'badge-info',
        IN_PROGRESS:      'badge-primary',
        MERGED:           'badge-success',
        FAILED:           'badge-danger',
      }[doc.pipeline_status] || 'badge-secondary';

      row.innerHTML = `
        <td>
          <a href="/admin/documents/${doc.doc_ref}/"
             class="doc-link">${doc.title}</a>
        </td>
        <td>
          <span class="badge ${statusClass}">
            ${doc.pipeline_status.replace(/_/g,' ')}
          </span>
        </td>
        <td>
          <div class="mini-progress">
            <div class="mini-progress-fill"
                 style="width:${doc.progress_pct}%"></div>
          </div>
          <small>${doc.approved_pages}/${doc.total_pages} pages</small>
        </td>
        <td>${doc.uploaded_by}</td>
        <td>${this._timeAgo(doc.uploaded_at)}</td>
      `;
    });
  }

  _updateQueueDisplay(data) {
    // Update queue count badge
    const badge = document.getElementById('queue-count-badge');
    if (badge) {
      const prev  = parseInt(badge.textContent) || 0;
      const count = data.queue_count || 0;
      badge.textContent = count;

      if (count > prev) {
        badge.classList.add('badge-pulse');
        setTimeout(() => badge.classList.remove('badge-pulse'), 2000);
      }
      badge.classList.toggle('badge-has-items', count > 0);
    }

    // Admin: update unassigned queue list
    if (data.role === 'admin' && data.queue) {
      const list = document.getElementById('unassigned-queue-list');
      if (!list) return;
      this._setText('unassigned-count', data.queue_count);
    }
  }

  _updateSubmittedQueue(data) {
    // Update review badge
    const badge = document.getElementById('review-queue-badge');
    if (badge) {
      badge.textContent = data.pending_count || 0;
      badge.classList.toggle('badge-has-items', data.pending_count > 0);
    }

    // Update review queue table
    const table = document.getElementById('review-queue-table');
    const tbody = table ? table.querySelector('tbody') : null;
    if (!tbody || !data.pending_review) return;

    // Add new submissions at top
    data.pending_review.forEach(s => {
      const existing = tbody.querySelector(
        `[data-submission-id="${s.id}"]`
      );
      if (!existing) {
        const row = document.createElement('tr');
        row.setAttribute('data-submission-id', s.id);
        row.innerHTML = `
          <td>${s.doc_title}</td>
          <td>Page ${s.page_number}</td>
          <td>${s.submitted_by}</td>
          <td>${this._timeAgo(s.submitted_at)}</td>
          <td>
            <a href="${s.review_url}" class="btn btn-sm btn-primary">
              Review
            </a>
          </td>
        `;
        tbody.insertBefore(row, tbody.firstChild);
        // Flash new item
        row.classList.add('row-flash-new');
        setTimeout(() => row.classList.remove('row-flash-new'), 2000);

        // Toast notification for new submission
        this._toast(
          `${s.submitted_by} submitted Page ${s.page_number} of `
          + `${s.doc_title}`,
          'info'
        );
      }
    });
  }

  // ── UI update methods — Resource ──────────────────────────

  _updateAssignmentList(data) {
    if (data.role !== 'resource') return;

    const list = document.getElementById('assignment-list');
    if (!list) return;

    const prevCount = list.querySelectorAll('.assignment-item').length;
    list.innerHTML  = '';

    if (!data.assignments || data.assignments.length === 0) {
      list.innerHTML = `
        <div class="no-assignments">
          <div class="empty-icon">📭</div>
          <p>No pages assigned yet.</p>
          <p class="text-muted">New work will appear here automatically.</p>
        </div>
      `;
      return;
    }

    data.assignments.forEach(a => {
      const el = document.createElement('div');
      el.className = 'assignment-item';
      el.setAttribute('data-assignment-id', a.assignment_id);

      const complexityColor = {
        SIMPLE:      '#22c55e',
        COMPLEX:     '#f59e0b',
        TABLE_HEAVY: '#ef4444',
      }[a.complexity] || '#888';

      el.innerHTML = `
        <div class="assignment-header">
          <span class="doc-title">${a.doc_title}</span>
          <span class="complexity-badge"
                style="background:${complexityColor}20;
                       color:${complexityColor};
                       border:1px solid ${complexityColor}40">
            ${a.complexity}
          </span>
        </div>
        <div class="assignment-meta">
          Page ${a.page_number} &middot;
          Max ${Math.floor((a.max_time||300)/60)} min
        </div>
        <a href="${a.workspace_url}"
           class="btn-open-workspace">
          Open Workspace →
        </a>
      `;

      list.appendChild(el);
    });

    // New assignment appeared — notify
    if (data.assignments.length > prevCount && prevCount >= 0) {
      const newCount = data.assignments.length - prevCount;
      this._toast(
        `${newCount} new page${newCount > 1 ? 's' : ''} assigned to you!`,
        'success'
      );
    }

    // Update load display
    const loadEl = document.getElementById('current-load-display');
    if (loadEl && data.current_load !== undefined) {
      loadEl.textContent = `${data.current_load.toFixed(1)} / ${data.remaining?.toFixed(1) || '?'} remaining`;
    }
  }

  _updateOnlineIndicator(isOnline) {
    const dot  = document.getElementById('online-status-dot');
    const text = document.getElementById('online-status-text');
    if (dot) {
      dot.className = isOnline ? 'status-dot status-online' : 'status-dot status-offline';
    }
    if (text) text.textContent = isOnline ? 'Online' : 'Reconnecting...';
  }

  // ── Utilities ─────────────────────────────────────────────

  _setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  }

  _timeAgo(isoString) {
    if (!isoString) return 'Never';
    const diff = (Date.now() - new Date(isoString)) / 1000;
    if (diff < 60)   return 'just now';
    if (diff < 3600) return `${Math.floor(diff/60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff/3600)}h ago`;
    return `${Math.floor(diff/86400)}d ago`;
  }

  _toast(message, type = 'info') {
    const colors = {
      success: '#22c55e',
      info:    '#3b82f6',
      warning: '#f59e0b',
      error:   '#ef4444',
    };
    const t = document.createElement('div');
    t.style.cssText = `
      position:fixed; bottom:24px; right:24px;
      background:#1a1a1c;
      border-left:4px solid ${colors[type]};
      border:1px solid rgba(255,255,255,0.08);
      border-left-width:4px;
      border-left-color:${colors[type]};
      border-radius:8px; padding:12px 18px;
      font-size:13px; color:#f0f0f2;
      font-family:'IBM Plex Mono',monospace;
      z-index:9999; max-width:360px;
      box-shadow:0 8px 32px rgba(0,0,0,0.4);
      transform:translateY(80px); opacity:0;
      transition:transform 0.3s cubic-bezier(0.34,1.56,0.64,1),
                 opacity 0.2s ease;
    `;
    t.textContent = message;
    document.body.appendChild(t);
    requestAnimationFrame(() => {
      t.style.transform = 'translateY(0)';
      t.style.opacity   = '1';
    });
    setTimeout(() => {
      t.style.transform = 'translateY(80px)';
      t.style.opacity   = '0';
      setTimeout(() => t.remove(), 300);
    }, 4000);
  }

  getCsrf() {
    return document.cookie
      .split(';')
      .find(c => c.trim().startsWith('docpro_csrftoken='))
      ?.split('=')?.[1] || 
      document.cookie
      .split(';')
      .find(c => c.trim().startsWith('csrftoken='))
      ?.split('=')?.[1] || '';
  }

  _injectStyles() {
    if (document.getElementById('realtime-styles')) return;
    const realtimeCSS = `
    /* Online status dot */
    .status-dot {
      display:inline-block;
      width:8px; height:8px;
      border-radius:50%;
      margin-right:6px;
      flex-shrink:0;
    }
    .status-online  { background:#22c55e;
                      box-shadow:0 0 6px rgba(34,197,94,0.6); }
    .status-away    { background:#f59e0b;
                      box-shadow:0 0 6px rgba(245,158,11,0.5); }
    .status-offline { background:#6b7280; }

    /* Capacity bar */
    .capacity-bar-wrap {
      width:80px; height:6px;
      background:rgba(255,255,255,0.08);
      border-radius:9999px; overflow:hidden;
      display:inline-block; vertical-align:middle;
      margin-right:6px;
    }
    .capacity-bar-fill {
      height:100%; border-radius:9999px;
      background:#3b82f6;
      transition:width 0.4s ease;
    }

    /* Queue count badge */
    .queue-badge {
      display:inline-flex; align-items:center;
      justify-content:center;
      min-width:20px; height:20px;
      padding:0 6px; border-radius:9999px;
      font-size:11px; font-weight:700;
      background:rgba(255,255,255,0.08);
      color:#888; transition:all 0.2s;
    }
    .queue-badge.badge-has-items {
      background:rgba(59,130,246,0.2);
      color:#60a5fa;
      border:1px solid rgba(59,130,246,0.3);
    }
    .queue-badge.badge-pulse {
      animation:badge-pop 0.4s cubic-bezier(0.34,1.56,0.64,1);
    }
    @keyframes badge-pop {
      0%   { transform:scale(1); }
      50%  { transform:scale(1.4); }
      100% { transform:scale(1); }
    }

    /* New row flash */
    @keyframes flash-new {
      0%,100% { background:transparent; }
      50%     { background:rgba(59,130,246,0.12); }
    }
    .row-flash-new { animation:flash-new 1s ease 2; }

    /* Mini progress bar */
    .mini-progress {
      width:100px; height:5px;
      background:rgba(255,255,255,0.07);
      border-radius:9999px; overflow:hidden;
      margin-bottom:3px;
    }
    .mini-progress-fill {
      height:100%; border-radius:9999px;
      background:linear-gradient(90deg,#3b82f6,#60a5fa);
      transition:width 0.5s ease;
    }

    /* Stat counters */
    .stat-number {
      font-family:'IBM Plex Mono',monospace;
      font-size:28px; font-weight:700;
      color:#f0f0f2; transition:color 0.3s;
    }
    `;
    const styleTag = document.createElement('style');
    styleTag.id = 'realtime-styles';
    styleTag.textContent = realtimeCSS;
    document.head.appendChild(styleTag);
  }
}
