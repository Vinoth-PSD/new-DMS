/**
 * ABBYY FineReader Style OCR Editor Logic
 */

class AbbyyEditor {
    constructor(config) {
        this.config = config;
        this.docRef = config.docRef;
        this.assignmentId = config.assignmentId;
        this.isReadonly = config.isReadonly;
        
        this.pages = [];
        this.currentPageIndex = 0;
        this.zoom = 1.0;
        this.activeTool = 'select'; // 'select', 'text', 'table', 'picture'
        
        this.pdfDoc = null;
        this.socket = null;
        
        // Table Selection State
        this.selectedTable = null; // { id, table_ref, row, col }
        this.ctxMenu = document.getElementById('table-context-menu');
        this.tableOpsGroup = document.getElementById('rbn-table-ops');
        
        // Transformation State
        this.transforming = null; // { element, type, startX, startY, startLeft, startTop, startWidth, startHeight, blockData }
        
        // DOM Elements
        this.thumbnailList = document.getElementById('thumbnail-list');
        this.imagePane = document.getElementById('image-pane-content');
        this.editorPane = document.getElementById('editor-pane-content');
        this.pageIndicator = document.getElementById('page-indicator');
        this.statusPageInfo = document.getElementById('status-page-info');
        
        this.init();
    }

    async init() {
        console.log("Initializing ABBYY Editor...");
        this.setupEventListeners();
        this.connectWebSocket();
        await this.loadWorkspaceData();
        this.renderThumbnails();
        await this.loadPage(0);
    }

    async loadWorkspaceData() {
        try {
            const resp = await fetch(`/api/v1/processing/content/${this.docRef}/1/`);
            const data = await resp.json();
            this.pages = data.pages;
            
            // If PDF URL is available, load it once
            if (data.document && data.document.document_url) {
                const loadingTask = pdfjsLib.getDocument(data.document.document_url);
                this.pdfDoc = await loadingTask.promise;
            }
        } catch (err) {
            console.error("Failed to load workspace data:", err);
            this.showToast("Error loading document data", "danger");
        }
    }

    setupEventListeners() {
        // Toolbar Buttons
        document.querySelectorAll('.toolbar-btn[data-tool]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.toolbar-btn[data-tool]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.activeTool = btn.dataset.tool;
                console.log("Active Tool:", this.activeTool);
            });
        });

        // Action Buttons
        document.getElementById('btn-analyze')?.addEventListener('click', () => this.analyzeCurrentPage());
        document.getElementById('btn-recognize')?.addEventListener('click', () => this.recognizeCurrentPage());
        document.getElementById('btn-save')?.addEventListener('click', () => this.saveDocument());
        document.getElementById('btn-submit')?.addEventListener('click', () => this.showSubmitModal());
        
        document.getElementById('confirm-submit')?.addEventListener('click', () => this.submitBlock());
        
        // Table Operations
        document.getElementById('btn-add-row')?.addEventListener('click', () => this.addRow());
        document.getElementById('btn-add-col')?.addEventListener('click', () => this.addColumn());
        document.getElementById('btn-del-row')?.addEventListener('click', () => this.deleteRow());
        document.getElementById('btn-del-col')?.addEventListener('click', () => this.deleteColumn());

        // Context Menu Handlers
        document.getElementById('ctx-add-row-above')?.addEventListener('click', () => this.addRow('above'));
        document.getElementById('ctx-add-row-below')?.addEventListener('click', () => this.addRow('below'));
        document.getElementById('ctx-add-col-left')?.addEventListener('click', () => this.addColumn('left'));
        document.getElementById('ctx-add-col-right')?.addEventListener('click', () => this.addColumn('right'));
        document.getElementById('ctx-del-row')?.addEventListener('click', () => this.deleteRow());
        document.getElementById('ctx-del-col')?.addEventListener('click', () => this.deleteColumn());

        // Hide context menu on click elsewhere
        document.addEventListener('click', (e) => {
            if (this.ctxMenu) this.ctxMenu.style.display = 'none';
        });

        // Zoom Controls
        document.getElementById('zoom-in')?.addEventListener('click', () => { this.zoom += 0.1; this.loadPage(this.currentPageIndex); });
        document.getElementById('zoom-out')?.addEventListener('click', () => { this.zoom -= 0.1; this.loadPage(this.currentPageIndex); });
        
        // Draggable Divider (Simulated for now)
        const divider = document.querySelector('.pane-divider');
        if (divider) {
            let isDragging = false;
            divider.addEventListener('mousedown', () => isDragging = true);
            document.addEventListener('mousemove', (e) => {
                if (!isDragging) return;
                const leftPane = document.getElementById('image-pane');
                const percentage = (e.clientX / window.innerWidth) * 100;
                if (percentage > 10 && percentage < 90) {
                    leftPane.style.flex = `0 0 ${percentage}%`;
                }
            });
            document.addEventListener('mouseup', () => isDragging = false);
        }
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        this.socket = new WebSocket(
            `${protocol}${window.location.host}/ws/workspace/${this.docRef}/1/`
        );

        this.socket.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.type === 'save_confirmation') {
                this.updateSaveStatus('All changes saved', 'success');
            }
        };
    }

    renderThumbnails() {
        this.thumbnailList.innerHTML = '';
        this.pages.forEach((page, index) => {
            const item = document.createElement('div');
            item.className = `thumbnail-item ${index === this.currentPageIndex ? 'active' : ''}`;
            item.innerHTML = `
                <div class="thumbnail-img">PAGE ${page.page_number}</div>
                <div class="thumbnail-label">${page.page_number}</div>
            `;
            item.onclick = () => this.loadPage(index);
            this.thumbnailList.appendChild(item);
        });
    }

    async loadPage(index) {
        this.currentPageIndex = index;
        const pageData = this.pages[index];
        console.log(`Loading Page ${pageData.page_number}...`);

        // Update UI
        this.pageIndicator.innerText = `Page ${pageData.page_number} of ${this.pages.length}`;
        this.statusPageInfo.innerText = `Page: ${pageData.page_number} / ${this.pages.length}`;
        
        document.querySelectorAll('.thumbnail-item').forEach((item, idx) => {
            item.classList.toggle('active', idx === index);
        });

        // 1. Render Image Pane (PDF)
        await this.renderPdfPage(pageData);

        // 2. Render Editor Pane (OCR Results)
        this.renderEditorContent(pageData);
    }

    async renderPdfPage(pageData) {
        this.imagePane.innerHTML = '';
        const pageContainer = document.createElement('div');
        pageContainer.className = 'page-container';
        this.imagePane.appendChild(pageContainer);

        const canvas = document.createElement('canvas');
        canvas.className = 'canvas-layer';
        pageContainer.appendChild(canvas);

        const areaLayer = document.createElement('div');
        areaLayer.className = 'area-layer';
        pageContainer.appendChild(areaLayer);

        try {
            const pdfPage = await this.pdfDoc.getPage(pageData.page_number);
            const baseWidth = 800;
            const viewport = pdfPage.getViewport({ scale: (baseWidth / pdfPage.view[2]) * this.zoom });
            
            canvas.width = viewport.width;
            canvas.height = viewport.height;
            pageContainer.style.width = viewport.width + 'px';
            pageContainer.style.height = viewport.height + 'px';

            const renderContext = {
                canvasContext: canvas.getContext('2d'),
                viewport: viewport
            };
            await pdfPage.render(renderContext).promise;

            // Render Areas
            this.renderAreas(areaLayer, pageData, viewport);
            
            // Setup Drawing on Area Layer
            this.setupDrawing(areaLayer, pageData, viewport);

        } catch (err) {
            console.error("PDF Rendering failed:", err);
            pageContainer.innerHTML = '<div class="alert alert-danger">Failed to render PDF</div>';
        }
    }

    renderAreas(layer, pageData, viewport) {
        const scaleX = viewport.width / pageData.layout_data.page_dims.width;
        const scaleY = viewport.height / pageData.layout_data.page_dims.height;

        // Render Text Blocks as Areas
        pageData.blocks.forEach(block => {
            const rect = document.createElement('div');
            rect.className = `area-rect area-type-text`;
            rect.style.left = (block.x * scaleX) + 'px';
            rect.style.top = (block.y * scaleY) + 'px';
            rect.style.width = (block.width * scaleX) + 'px';
            rect.style.height = (block.height * scaleY) + 'px';
            rect.title = block.extracted_text;
            
            // Highlight sync
            rect.onmouseenter = () => this.highlightBlockInEditor(block.id);
            rect.onmouseleave = () => this.dimBlockInEditor(block.id);
            
            layer.appendChild(rect);
        });

        // Render Tables as Areas
        pageData.tables.forEach(table => {
            const rect = document.createElement('div');
            rect.className = `area-rect area-type-table`;
            rect.style.left = (table.x * scaleX) + 'px';
            rect.style.top = (table.y * scaleY) + 'px';
            rect.style.width = (table.width * scaleX) + 'px';
            rect.style.height = (table.height * scaleY) + 'px';
            rect.dataset.id = table.id;
            rect.dataset.type = 'table';
            
            if (!this.isReadonly) {
                this.addResizeHandles(rect, table, scaleX, scaleY);
                this.renderGridLines(rect, table);
                
                rect.onmousedown = (e) => {
                    if (this.activeTool !== 'select') return;
                    if (e.target !== rect) return; 
                    this.initInteraction(e, rect, 'move', table, scaleX, scaleY);
                    this.renderFloatingToolbar(rect, table);
                };
            }
            
            layer.appendChild(rect);
        });
    }

    renderGridLines(rect, table) {
        // Remove existing grid lines
        rect.querySelectorAll('.grid-line').forEach(l => l.remove());

        // Vertical lines (Columns)
        if (table.col_widths && table.col_widths.length > 1) {
            let currentLeft = 0;
            for (let i = 0; i < table.col_widths.length - 1; i++) {
                currentLeft += table.col_widths[i];
                const line = document.createElement('div');
                line.className = 'grid-line vertical';
                line.style.left = currentLeft + '%';
                line.onmousedown = (e) => {
                    e.stopPropagation();
                    this.initGridInteraction(e, rect, 'col', i, table);
                };
                rect.appendChild(line);
            }
        }

        // Horizontal lines (Rows)
        if (table.row_heights && table.row_heights.length > 1) {
            let currentTop = 0;
            for (let i = 0; i < table.row_heights.length - 1; i++) {
                currentTop += table.row_heights[i];
                const line = document.createElement('div');
                line.className = 'grid-line horizontal';
                line.style.top = currentTop + '%';
                line.onmousedown = (e) => {
                    e.stopPropagation();
                    this.initGridInteraction(e, rect, 'row', i, table);
                };
                rect.appendChild(line);
            }
        }
    }

    initGridInteraction(e, rect, type, index, table) {
        const startX = e.clientX;
        const startY = e.clientY;
        const rectBox = rect.getBoundingClientRect();
        
        const onMouseMove = (me) => {
            if (type === 'col') {
                const dx = ((me.clientX - startX) / rectBox.width) * 100;
                const newWidth1 = Math.max(5, table.col_widths[index] + dx);
                const newWidth2 = Math.max(5, table.col_widths[index + 1] - (newWidth1 - table.col_widths[index]));
                
                if (newWidth1 > 5 && newWidth2 > 5) {
                    const diff = newWidth1 - table.col_widths[index];
                    table.col_widths[index] = newWidth1;
                    table.col_widths[index + 1] -= diff;
                    this.renderGridLines(rect, table);
                }
            } else {
                const dy = ((me.clientY - startY) / rectBox.height) * 100;
                const newHeight1 = Math.max(5, table.row_heights[index] + dy);
                const newHeight2 = Math.max(5, table.row_heights[index + 1] - (newHeight1 - table.row_heights[index]));
                
                if (newHeight1 > 5 && newHeight2 > 5) {
                    const diff = newHeight1 - table.row_heights[index];
                    table.row_heights[index] = newHeight1;
                    table.row_heights[index + 1] -= diff;
                    this.renderGridLines(rect, table);
                }
            }
            // Update start points for smooth incremental movement
            // startX = me.clientX; // Wait, actually standard drag usually uses initial startX
            // Let's keep it cumulative for now but maybe reset it if it feels jumpy
        };

        const onMouseUp = async () => {
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
            await this.persistTableUpdate(table);
        };

        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    }

    renderFloatingToolbar(rect, table) {
        this.hideFloatingToolbar();
        const toolbar = document.createElement('div');
        toolbar.className = 'area-toolbar';
        toolbar.innerHTML = `
            <button title="Add Row Below" onclick="window.editor.addRow('${table.id}', -1)"><i class="fas fa-plus-square"></i></button>
            <button title="Add Column Right" onclick="window.editor.addColumn('${table.id}', -1)"><i class="fas fa-plus-circle"></i></button>
            <button title="Run OCR on Table" onclick="window.editor.runOCROnTable('${table.id}')"><i class="fas fa-magic"></i></button>
            <button title="Delete Table" class="danger" onclick="window.editor.deleteTable('${table.id}')"><i class="fas fa-trash"></i></button>
        `;
        rect.appendChild(toolbar);
        this.activeToolbar = toolbar;
    }

    hideFloatingToolbar() {
        if (this.activeToolbar) {
            this.activeToolbar.remove();
            this.activeToolbar = null;
        }
    }

    addResizeHandles(rect, data, scaleX, scaleY) {
        const positions = ['n', 's', 'e', 'w', 'nw', 'ne', 'sw', 'se'];
        positions.forEach(pos => {
            const h = document.createElement('div');
            h.className = `resizer ${pos}`;
            h.onmousedown = (e) => {
                e.stopPropagation();
                this.initInteraction(e, rect, pos, data, scaleX, scaleY);
            };
            rect.appendChild(h);
        });
    }

    initInteraction(e, rect, type, data, scaleX, scaleY) {
        this.transforming = {
            element: rect,
            type: type,
            startX: e.clientX,
            startY: e.clientY,
            startLeft: parseFloat(rect.style.left),
            startTop: parseFloat(rect.style.top),
            startWidth: parseFloat(rect.style.width),
            startHeight: parseFloat(rect.style.height),
            data: data,
            scaleX: scaleX,
            scaleY: scaleY
        };

        const onMouseMove = (me) => {
            if (!this.transforming) return;
            const dx = me.clientX - this.transforming.startX;
            const dy = me.clientY - this.transforming.startY;
            const t = this.transforming;

            if (t.type === 'move') {
                rect.style.left = (t.startLeft + dx) + 'px';
                rect.style.top = (t.startTop + dy) + 'px';
            } else {
                // Resize logic
                if (t.type.includes('e')) rect.style.width = Math.max(20, t.startWidth + dx) + 'px';
                if (t.type.includes('w')) {
                    const newWidth = Math.max(20, t.startWidth - dx);
                    if (newWidth > 20) {
                        rect.style.width = newWidth + 'px';
                        rect.style.left = (t.startLeft + dx) + 'px';
                    }
                }
                if (t.type.includes('s')) rect.style.height = Math.max(20, t.startHeight + dy) + 'px';
                if (t.type.includes('n')) {
                    const newHeight = Math.max(20, t.startHeight - dy);
                    if (newHeight > 20) {
                        rect.style.height = newHeight + 'px';
                        rect.style.top = (t.startTop + dy) + 'px';
                    }
                }
            }
        };

        const onMouseUp = async () => {
            if (!this.transforming) return;
            const t = this.transforming;
            this.transforming = null;
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);

            // Update data object with new scaled coordinates
            t.data.x = parseFloat(rect.style.left) / t.scaleX;
            t.data.y = parseFloat(rect.style.top) / t.scaleY;
            t.data.width = parseFloat(rect.style.width) / t.scaleX;
            t.data.height = parseFloat(rect.style.height) / t.scaleY;

            if (t.data.table_ref) {
                await this.persistTableUpdate(t.data);
            } else {
                // If it's a block, we'd need a separate block update call
                // For now, let's focus on tables as requested
            }
        };

        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    }

    renderEditorContent(pageData) {
        this.editorPane.innerHTML = '';
        const editorContent = document.createElement('div');
        editorContent.className = 'editor-content';
        this.editorPane.appendChild(editorContent);

        // Map dimensions from layout_data
        const pdfWidth = pageData.layout_data.page_dims.width;
        const pdfHeight = pageData.layout_data.page_dims.height;
        const baseWidth = 800; // Match left pane for initial calc
        const scale = (baseWidth / pdfWidth) * this.zoom;

        editorContent.style.width = (pdfWidth * scale) + 'px';
        editorContent.style.height = (pdfHeight * scale) + 'px';

        // Render blocks at precise locations
        pageData.blocks.forEach(block => {
            const blockDiv = document.createElement('div');
            blockDiv.className = 'structured-block';
            blockDiv.id = `editor-block-${block.id}`;
            blockDiv.contentEditable = !this.isReadonly;
            blockDiv.dataset.id = block.id;
            
            blockDiv.style.left = (block.x * scale) + 'px';
            blockDiv.style.top = (block.y * scale) + 'px';
            blockDiv.style.width = (block.width * scale) + 'px';
            blockDiv.style.height = (block.height * scale) + 'px';
            
            blockDiv.style.fontFamily = block.font_name || 'serif';
            // Scale font size too
            const fontSize = (block.font_size || 11) * scale * 1.33; // pt to px approx
            blockDiv.style.fontSize = fontSize + 'px';
            
            if (block.font_weight === 'bold') blockDiv.style.fontWeight = 'bold';
            if (block.font_style === 'italic') blockDiv.style.fontStyle = 'italic';
            
            blockDiv.innerText = block.current_text || block.extracted_text || "";
            
            blockDiv.oninput = () => {
                this.updateSaveStatus('Typing...', 'warning');
                this.debouncedSave(block.id, blockDiv.innerText);
            };
            
            // Sync highlight to left
            blockDiv.onfocus = () => this.highlightAreaOnLeft(block.id);
            
            editorContent.appendChild(blockDiv);
        });
        
        // Render tables
        pageData.tables.forEach(table => {
            const tableEl = document.createElement('div');
            tableEl.className = 'area-rect area-type-table'; // Reuse style or similar
            tableEl.style.position = 'absolute';
            tableEl.style.left = (table.x * scale) + 'px';
            tableEl.style.top = (table.y * scale) + 'px';
            tableEl.style.width = (table.width * scale) + 'px';
            tableEl.style.height = (table.height * scale) + 'px';
            tableEl.style.background = 'white';
            tableEl.style.border = '1px solid #2563eb';
            
            const realTable = document.createElement('table');
            realTable.style.width = '100%';
            realTable.style.height = '100%';
            realTable.style.borderCollapse = 'collapse';
            
            table.table_json.forEach((row, r_idx) => {
                const tr = document.createElement('tr');
                row.forEach((cell, c_idx) => {
                    const td = document.createElement('td');
                    td.style.border = '1px solid #ddd';
                    td.style.padding = '2px';
                    td.style.fontSize = (10 * scale) + 'px';
                    td.innerText = cell;
                    td.contentEditable = !this.isReadonly;
                    
                    td.oninput = () => {
                        this.updateSaveStatus('Typing...', 'warning');
                        this.debouncedSaveTableCell(table.table_ref, r_idx, c_idx, td.innerText);
                    };

                    td.oncontextmenu = (e) => {
                        if (this.isReadonly) return;
                        e.preventDefault();
                        this.showTableContextMenu(e, table, r_idx, c_idx);
                    };

                    td.onfocus = () => {
                        this.selectedTable = { id: table.id, ref: table.table_ref, r: r_idx, c: c_idx, obj: table };
                        this.tableOpsGroup.style.display = 'block';
                    };
                    
                    tr.appendChild(td);
                });
                realTable.appendChild(tr);
            });
            tableEl.appendChild(realTable);
            editorContent.appendChild(tableEl);
        });
    }

    highlightAreaOnLeft(blockId) {
        // Implement reverse sync if needed
        console.log("Focusing block:", blockId);
    }

    highlightBlockInEditor(blockId) {
        const el = document.getElementById(`editor-block-${blockId}`);
        if (el) {
            el.style.backgroundColor = '#e7f3ff';
            el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
    }

    dimBlockInEditor(blockId) {
        const el = document.getElementById(`editor-block-${blockId}`);
        if (el) el.style.backgroundColor = 'transparent';
    }

    setupDrawing(layer, pageData, viewport) {
        if (this.isReadonly) return;

        let isDrawing = false;
        let startX, startY, currentRect;

        layer.onmousedown = (e) => {
            if (this.activeTool === 'select') return;
            
            isDrawing = true;
            const rect = layer.getBoundingClientRect();
            startX = e.clientX - rect.left;
            startY = e.clientY - rect.top;

            currentRect = document.createElement('div');
            currentRect.className = `area-rect area-type-${this.activeTool}`;
            currentRect.style.left = startX + 'px';
            currentRect.style.top = startY + 'px';
            layer.appendChild(currentRect);
        };

        window.onmousemove = (e) => {
            if (!isDrawing) return;
            const rect = layer.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;

            const width = Math.abs(x - startX);
            const height = Math.abs(y - startY);
            const left = Math.min(x, startX);
            const top = Math.min(y, startY);

            currentRect.style.width = width + 'px';
            currentRect.style.height = height + 'px';
            currentRect.style.left = left + 'px';
            currentRect.style.top = top + 'px';
        };

        window.onmouseup = () => {
            if (!isDrawing) return;
            isDrawing = false;
            
            const rect = layer.getBoundingClientRect();
            const width = parseFloat(currentRect.style.width);
            const height = parseFloat(currentRect.style.height);
            const left = parseFloat(currentRect.style.left);
            const top = parseFloat(currentRect.style.top);

            if (width < 10 || height < 10) {
                currentRect.remove();
                return;
            }

            const scaleX = viewport.width / pageData.layout_data.page_dims.width;
            const scaleY = viewport.height / pageData.layout_data.page_dims.height;

            const blockData = {
                type: this.activeTool,
                x: left / scaleX,
                y: top / scaleY,
                width: width / scaleX,
                height: height / scaleY,
                row_count: 3, // Default for manual tables
                col_count: 3
            };

            this.createBlockOnBackend(pageData.id, blockData, currentRect);
        };
    }

    async createBlockOnBackend(pageId, data, tempRect) {
        this.updateSaveStatus('Creating area...', 'warning');
        try {
            const resp = await fetch(`/api/v1/processing/pages/${pageId}/blocks/create/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': this.getCookie('csrftoken')
                },
                body: JSON.stringify(data)
            });
            if (resp.ok) {
                const result = await resp.json();
                this.showToast("Area created", "success");
                // Refresh to get the real block with ID
                this.loadPage(this.currentPageIndex);
            } else {
                tempRect.remove();
            }
        } catch (err) {
            tempRect.remove();
            this.showToast("Failed to create area", "danger");
        }
    }

    async analyzeCurrentPage() {
        const page = this.pages[this.currentPageIndex];
        this.updateSaveStatus('Analyzing layout...', 'warning');
        try {
            const resp = await fetch(`/api/v1/processing/pages/${page.id}/analyze/`, {
                method: 'POST',
                headers: { 'X-CSRFToken': this.getCookie('csrftoken') }
            });
            if (resp.ok) {
                this.showToast("Layout analysis started", "success");
            }
        } catch (err) {
            this.showToast("Analysis failed", "danger");
        }
    }

    async recognizeCurrentPage() {
        const page = this.pages[this.currentPageIndex];
        this.updateSaveStatus('Recognizing text...', 'warning');
        try {
            const resp = await fetch(`/api/v1/processing/pages/${page.id}/recognize/`, {
                method: 'POST',
                headers: { 'X-CSRFToken': this.getCookie('csrftoken') }
            });
            if (resp.ok) {
                this.showToast("OCR recognition started", "success");
            }
        } catch (err) {
            this.showToast("Recognition failed", "danger");
        }
    }

    debouncedSave(blockId, text) {
        if (this.saveTimer) clearTimeout(this.saveTimer);
        this.saveTimer = setTimeout(() => {
            this.saveBlock(blockId, text);
        }, 1000);
    }

    async saveBlock(blockId, text) {
        try {
            const resp = await fetch(`/api/v1/processing/blocks/${blockId}/save/`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': this.getCookie('csrftoken')
                },
                body: JSON.stringify({ text: text })
            });
            if (resp.ok) {
                this.updateSaveStatus('Changes saved', 'success');
            }
        } catch (err) {
            this.updateSaveStatus('Save Error', 'danger');
        }
    }

    debouncedSaveTableCell(tableId, row, col, text) {
        if (this.tableSaveTimer) clearTimeout(this.tableSaveTimer);
        this.tableSaveTimer = setTimeout(() => {
            this.saveTableCell(tableId, row, col, text);
        }, 1000);
    }

    async saveTableCell(tableId, row, col, text) {
        try {
            const resp = await fetch(`/api/v1/processing/tables/${tableId}/cell/save/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': this.getCookie('csrftoken')
                },
                body: JSON.stringify({
                    row: row,
                    col: col,
                    text: text
                })
            });
            if (resp.ok) {
                this.updateSaveStatus('Table saved', 'success');
            }
        } catch (err) {
            this.updateSaveStatus('Table Save Error', 'danger');
        }
    }

    saveDocument() {
        this.showToast("Saving all pages...", "info");
        // Trigger bulk save if needed
    }

    showSubmitModal() {
        document.getElementById('submit-modal').style.display = 'flex';
    }

    async submitBlock() {
        document.getElementById('submit-modal').style.display = 'none';
        this.updateSaveStatus('Submitting...', 'warning');
        
        try {
            const resp = await fetch(`/api/v1/processing/content/${this.docRef}/${this.pages[0].page_number}/submit/`, {
                method: 'POST',
                headers: { 'X-CSRFToken': this.getCookie('csrftoken') }
            });
            if (resp.ok) {
                this.showToast("Work submitted successfully!", "success");
                setTimeout(() => {
                    const backUrl = this.config.role === 'ADMIN' ? "/admin/documents/" : "/resource/fetch/";
                    window.location.href = backUrl;
                }, 1500);
            }
        } catch (err) {
            this.showToast("Submission failed", "danger");
        }
    }

    updateSaveStatus(msg, type) {
        const el = document.getElementById('save-status');
        if (el) {
            el.innerText = msg;
            el.className = `text-${type} me-3`;
        }
    }

    showToast(msg, type) {
        console.log(`[Toast] ${type}: ${msg}`);
        // Implement professional toast if available
        const status = document.getElementById('save-status');
        if (status) {
            status.innerText = msg;
            status.className = `text-${type} me-3`;
        }
    }

    // --- Table Manipulation Logic ---

    showTableContextMenu(e, table, row, col) {
        this.selectedTable = { id: table.id, ref: table.table_ref, r: row, c: col, obj: table };
        this.ctxMenu.style.display = 'block';
        this.ctxMenu.style.left = e.clientX + 'px';
        this.ctxMenu.style.top = e.clientY + 'px';
        this.tableOpsGroup.style.display = 'block';
    }

    async addRow(position = 'below') {
        if (!this.selectedTable) return;
        const { obj, r } = this.selectedTable;
        const newRow = Array(obj.col_count).fill({ text: '', indent: 0 });
        const insertAt = position === 'above' ? r : r + 1;
        
        obj.table_json.splice(insertAt, 0, newRow);
        obj.row_count++;

        if (obj.row_heights && obj.row_heights.length > 0) {
            const avgHeight = 100.0 / obj.row_count;
            obj.row_heights.splice(insertAt, 0, avgHeight);
            // Normalize
            const sum = obj.row_heights.reduce((a, b) => a + b, 0);
            obj.row_heights = obj.row_heights.map(h => (h / sum) * 100);
        } else {
            obj.row_heights = Array(obj.row_count).fill(100.0 / obj.row_count);
        }

        await this.persistTableUpdate(obj);
    }

    async deleteRow() {
        if (!this.selectedTable) return;
        const { obj, r } = this.selectedTable;
        if (obj.row_count <= 1) return;

        obj.table_json.splice(r, 1);
        obj.row_count--;
        
        if (obj.row_heights && obj.row_heights.length > 0) {
            obj.row_heights.splice(r, 1);
            const sum = obj.row_heights.reduce((a, b) => a + b, 0);
            obj.row_heights = obj.row_heights.map(h => (h / sum) * 100);
        }

        await this.persistTableUpdate(obj);
    }

    async addColumn(position = 'right') {
        if (!this.selectedTable) return;
        const { obj, c } = this.selectedTable;
        const insertAt = position === 'left' ? c : c + 1;

        obj.table_json.forEach(row => {
            row.splice(insertAt, 0, { text: '', indent: 0 });
        });
        obj.col_count++;

        if (obj.col_widths && obj.col_widths.length > 0) {
            const avgWidth = 100.0 / obj.col_count;
            obj.col_widths.splice(insertAt, 0, avgWidth);
            const sum = obj.col_widths.reduce((a, b) => a + b, 0);
            obj.col_widths = obj.col_widths.map(w => (w / sum) * 100);
        } else {
            obj.col_widths = Array(obj.col_count).fill(100.0 / obj.col_count);
        }

        await this.persistTableUpdate(obj);
    }

    async deleteColumn() {
        if (!this.selectedTable) return;
        const { obj, c } = this.selectedTable;
        if (obj.col_count <= 1) return;

        obj.table_json.forEach(row => {
            row.splice(c, 1);
        });
        obj.col_count--;

        if (obj.col_widths && obj.col_widths.length > 0) {
            obj.col_widths.splice(c, 1);
            const sum = obj.col_widths.reduce((a, b) => a + b, 0);
            obj.col_widths = obj.col_widths.map(w => (w / sum) * 100);
        }

        await this.persistTableUpdate(obj);
    }

    async runOCROnTable(tableId) {
        this.updateSaveStatus('Running OCR on table...', 'info');
        // This would call a backend task to perform OCR on the table's bbox
        // For now, we'll just mock a successful response
        setTimeout(() => {
            this.showToast("Table OCR complete (Mocked)", "success");
        }, 2000);
    }

    async deleteTable(tableId) {
        if (!confirm("Are you sure you want to delete this table?")) return;
        this.updateSaveStatus('Deleting table...', 'warning');
        try {
            const resp = await fetch(`/api/v1/processing/blocks/${tableId}/`, {
                method: 'DELETE',
                headers: { 'X-CSRFToken': this.getCookie('csrftoken') }
            });
            if (resp.ok) {
                this.showToast("Table deleted", "success");
                await this.loadPage(this.currentPageIndex);
            }
        } catch (err) {
            this.showToast("Failed to delete table", "danger");
        }
    }

    async persistTableUpdate(tableObj) {
        // Ensure coordinates are sent correctly to prevent shifting
        this.updateSaveStatus('Updating table...', 'warning');
        const pageId = this.pages[this.currentPageIndex].id;
        try {
            const resp = await fetch(`/api/v1/processing/pages/${pageId}/tables/save/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': this.getCookie('csrftoken')
                },
                body: JSON.stringify(tableObj)
            });
            if (resp.ok) {
                this.showToast("Table structure updated", "success");
                await this.loadPage(this.currentPageIndex); // Re-render
            }
        } catch (err) {
            this.showToast("Failed to update table", "danger");
        }
    }

    getCookie(name) {
        let cookieValue = null;
        if (document.cookie && document.cookie !== '') {
            const cookies = document.cookie.split(';');
            for (let i = 0; i < cookies.length; i++) {
                const cookie = cookies[i].trim();
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }
}

// Global initialization
window.AbbyyEditor = AbbyyEditor;
