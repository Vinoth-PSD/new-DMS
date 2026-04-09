/**
 * user_edit.js
 * Handles editing of resource credentials (username, email, max_capacity, password)
 */

document.addEventListener('DOMContentLoaded', function () {
    const editButtons = document.querySelectorAll('.action-btn[data-action="edit"]');
    const modal = document.getElementById('edit-user-modal');
    const form = document.getElementById('edit-user-form');
    const closeBtn = document.getElementById('close-edit-modal');

    if (!modal || !form) return;

    function getCsrfToken() {
        return (
            window.csrftoken ||
            document.querySelector('[name=csrfmiddlewaretoken]')?.value ||
            ''
        );
    }

    // Open Modal - Use delegation for dynamic table support
    document.addEventListener('click', function (e) {
        const button = e.target.closest('.action-btn[data-action="edit"]');
        if (!button) return;

        const userId = button.dataset.id;
        const username = button.dataset.username;
        const email = button.dataset.email;
        const capacity = button.dataset.capacity;

        document.getElementById('edit-user-id').value = userId;
        document.getElementById('edit-username').value = username;
        document.getElementById('edit-email').value = email;
        
        const capInput = document.getElementById('edit-capacity');
        if (capInput) capInput.value = capacity;
        
        document.getElementById('edit-password').value = ''; // Reset password field

        modal.style.display = 'flex';
    });

    // Close Modal
    closeBtn.onclick = function () {
        modal.style.display = 'none';
    };

    window.addEventListener('click', function (e) {
        if (e.target === modal) {
            modal.style.display = 'none';
        }
    });

    // Handle Form Submit
    form.addEventListener('submit', function (e) {
        e.preventDefault();

        const userId = document.getElementById('edit-user-id').value;
        const username = document.getElementById('edit-username').value;
        const email = document.getElementById('edit-email').value;
        const password = document.getElementById('edit-password').value;
        const capacity = document.getElementById('edit-capacity')?.value;

        const payload = {
            username: username,
            email: email
        };

        if (password && password.trim().length >= 8) {
            payload.password = password;
        }

        if (capacity) {
            payload.resource_profile = {
                max_capacity: parseInt(capacity)
            };
        }

        fetch(`/api/admin/users/${userId}/`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify(payload)
        })
        .then(response => {
            if (response.ok) {
                showToast('Resource updated successfully.', 'success');
                modal.style.display = 'none';
                if (window.fetchUserList) {
                    window.fetchUserList(window.userCurrentPage || 1);
                } else {
                    setTimeout(() => window.location.reload(), 1000);
                }
            } else {
                return response.json().then(data => {
                    const errorMsg = data.error || data.username || data.email || data.password || 'Failed to update user.';
                    showToast(errorMsg, 'error');
                });
            }
        })
        .catch(error => {
            console.error('Error updating user:', error);
            showToast('An error occurred. Please try again.', 'error');
        });
    });
});
