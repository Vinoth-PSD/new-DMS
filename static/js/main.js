// Main JS for DocPro


function getCookie(name) {
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

// Ensure CSRF token is available via a getter to stay fresh
Object.defineProperty(window, 'csrftoken', {
    get: function() { 
        return getCookie('docpro_csrftoken') || getCookie('csrftoken'); 
    }
});

// ── Global Toast Notification Utility ─────────────────
// Replaced by alert_system.html (showAlert / showToast)

// Global error handler for fetch
(function (originalFetch) {
    window.fetch = function () {
        return originalFetch.apply(this, arguments).then(function (response) {
            if (response.status === 401) {
                // Prevents infinite redirect loops if background polls fail while user is logged in
                if (window.isUserAuthenticated) {
                    console.warn('[Fetch] Background 401 encountered on authenticated page. Ignoring to prevent loop.');
                    return response;
                }
                if (!window.location.pathname.includes('/login/')) {
                    window.location.href = '/login/?next=' + window.location.pathname;
                }
            }
            return response;
        });
    };
})(window.fetch);

// Global Confirmation Utility
window.showConfirm = function (message, callback) {
    const modal = document.getElementById('confirm-modal');
    if (!modal) return;

    const msgElement = modal.querySelector('.modal-text');
    const confirmBtn = modal.querySelector('#confirm-yes');
    const cancelBtn = modal.querySelector('#confirm-no');

    msgElement.innerText = message;
    modal.style.display = 'flex';

    const hide = function () {
        modal.style.display = 'none';
        confirmBtn.onclick = null;
        cancelBtn.onclick = null;
    };

    confirmBtn.onclick = function () {
        hide();
        callback();
    };

    cancelBtn.onclick = hide;
};

// DOM Dependent Logic
const initApp = function () {
    function getCsrfToken() {
        return (
            window.csrftoken ||
            document.querySelector('[name=csrfmiddlewaretoken]')?.value ||
            getCookie('csrftoken') ||
            ''
        );
    }

    // 1. Sidebar Toggle Logic (Mobile)
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const sidebar = document.querySelector('.app-sidebar');

    if (sidebarToggle && sidebar) {
        let backdrop = document.querySelector('.sidebar-overlay');
        if (!backdrop) {
            backdrop = document.createElement('div');
            backdrop.className = 'sidebar-overlay';
            document.body.appendChild(backdrop);
        }

        function toggleSidebar() {
            sidebar.classList.toggle('show');
            backdrop.classList.toggle('show');
        }

        sidebarToggle.addEventListener('click', function (e) {
            e.stopPropagation();
            toggleSidebar();
        });

        backdrop.addEventListener('click', toggleSidebar);

        const links = sidebar.querySelectorAll('.sidebar-link');
        links.forEach(link => {
            link.addEventListener('click', () => {
                if (window.innerWidth < 992) {
                    toggleSidebar();
                }
            });
        });
    }

    // 2. User Management Actions (Platform Users Page) - Use delegation for dynamic table support
    document.addEventListener('click', function (e) {
        const button = e.target.closest('.action-btn');
        if (!button) return;

        const action = button.dataset.action;
        const userId = button.dataset.id;

        if (action === 'toggle') {
            const isActive = button.dataset.active === 'true';
            toggleUserStatus(userId, !isActive);
        } else if (action === 'delete') {
            const username = button.dataset.username;
            window.showConfirm(`Are you sure you want to delete user "${username}"?`, function () {
                deleteUser(userId);
            });
        }
    });

    function toggleUserStatus(userId, newStatus) {
        fetch(`/api/admin/users/${userId}/`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({ is_active: newStatus })
        })
            .then(response => {
                if (response.ok) {
                    showToast(`User status updated successfully.`, 'success');
                    if (window.fetchUserList) {
                        window.fetchUserList(window.userCurrentPage || 1);
                    } else {
                        setTimeout(() => window.location.reload(), 1000);
                    }
                } else {
                    return response.json().then(data => {
                        showToast(data.error || 'Failed to update user status.', 'error');
                    });
                }
            })
            .catch(error => {
                console.error('Error toggling user status:', error);
                showToast('An error occurred. Please try again.', 'error');
            });
    }

    function deleteUser(userId) {
        fetch(`/api/admin/users/${userId}/`, {
            method: 'DELETE',
            headers: {
                'X-CSRFToken': getCsrfToken()
            }
        })
            .then(response => {
                if (response.status === 204 || response.ok) {
                    showToast('User deleted successfully.', 'success');
                    if (window.fetchUserList) {
                        window.fetchUserList(window.userCurrentPage || 1);
                    } else {
                        setTimeout(() => window.location.reload(), 1000);
                    }
                } else {
                    return response.json().then(data => {
                        showToast(data.error || 'Failed to delete user.', 'error');
                    });
                }
            })
            .catch(error => {
                console.error('Error deleting user:', error);
                showToast('An error occurred. Please try again.', 'error');
            });
    }

    // 3. Real-time Presence & Notifications (Global Connection)
    // Optimized with exponential backoff and initial delay to prevent handshake congestion
    let notificationSocket = null;
    let reconnectDelay = 1000;
    const maxReconnectDelay = 30000;

    function initNotifications() {
        if (!window.location.protocol.includes('http')) return; 
        
        // Close existing if any
        if (notificationSocket) {
            notificationSocket.onclose = null;
            notificationSocket.close();
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = protocol + '//' + window.location.host + '/ws/notifications/';
        
        console.log(`Connecting to Notification WebSocket (Attempting in ${reconnectDelay}ms)...`);
        
        notificationSocket = new WebSocket(wsUrl);
        
        notificationSocket.onopen = function() {
            console.log('Notification WebSocket connected');
            reconnectDelay = 1000; // Reset on success
        };
        
        notificationSocket.onmessage = function(e) {
            try {
                const data = JSON.parse(e.data);
                if (data.type === 'admin_update') {
                     showToast(data.message, 'info');
                } else if (data.type === 'assignment_timeout') {
                     showToast(data.message, 'error', 0); 
                }
            } catch (err) {
                console.warn('WS Message Error:', err);
            }
        };
        
        notificationSocket.onclose = function(e) {
            console.log(`Notification WebSocket closed. Reconnecting in ${reconnectDelay}ms...`, e.reason);
            if (e.code === 4001 || e.code === 4003) {
                console.warn('WebSocket closed due to auth failure. Not reconnecting.');
                return;
            }
            setTimeout(initNotifications, reconnectDelay);
            // Exponential backoff
            reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
        };
        
        notificationSocket.onerror = function(err) {
            console.error('Notification WebSocket error:', err);
            notificationSocket.close();
        };
    }

    // 4. Heartbeat Logic (Consolidated from base.html)
    function startHeartbeat() {
        function sendHeartbeat() {
            fetch('/api/v1/auth/users/heartbeat/', {
                method: 'POST',
                headers: {
                    'X-CSRFToken': window.csrftoken,
                    'Content-Type': 'application/json'
                },
                credentials: 'include'
            })
            .then(response => {
                if (response.status === 401) {
                    if (!window.location.pathname.includes('/login/')) {
                        window.location.href = '/login/?next=' + window.location.pathname;
                    }
                }
            })
            .catch(err => console.debug('Heartbeat failed', err));
        }
        
        // Initial ping and then every 60 seconds
        sendHeartbeat();
        setInterval(sendHeartbeat, 60000);
    }

    // Initialize Global Systems if logged in
    if (window.isUserAuthenticated) {
        initNotifications();
        startHeartbeat();
    }
};

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}
