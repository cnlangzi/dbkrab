/**
 * dbkrab Dashboard - Common Utilities
 */

/**
 * Convert UTC time string to local timezone
 * @param {string} utcString - UTC time string (e.g., "2026-04-07 15:37:00")
 * @returns {string} Local time string in zh-CN format
 */
function formatLocalTime(utcString) {
    if (!utcString || utcString === 'null' || utcString === '-' || utcString === 'N/A' || utcString === 'Never') {
        return '-';
    }
    // Add 'Z' suffix if not present to indicate UTC
    const utcDate = utcString.endsWith('Z') ? utcString : utcString + 'Z';
    try {
        const date = new Date(utcDate);
        if (isNaN(date.getTime())) {
            return utcString;
        }
        return date.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
    } catch (e) {
        return utcString;
    }
}

/**
 * Convert all elements with .local-time class after HTMX loads content
 * Elements should have data-utc attribute with UTC time string
 */
function convertLocalTimes(container) {
    const selector = container ? container + ' .local-time' : '.local-time';
    document.querySelectorAll(selector).forEach(el => {
        const utcTime = el.getAttribute('data-utc');
        if (utcTime) {
            el.textContent = formatLocalTime(utcTime);
        }
    });
}

// Auto-convert on HTMX afterSwap event
document.body.addEventListener('htmx:afterSwap', function(evt) {
    // Convert times in the swapped content
    if (evt.detail.target) {
        const targetId = evt.detail.target.id;
        if (targetId) {
            convertLocalTimes('#' + targetId);
        } else {
            // Find .local-time elements in the target
            evt.detail.target.querySelectorAll('.local-time').forEach(el => {
                const utcTime = el.getAttribute('data-utc');
                if (utcTime) {
                    el.textContent = formatLocalTime(utcTime);
                }
            });
        }
    }
});

// Auto-convert on page load
document.addEventListener('DOMContentLoaded', function() {
    convertLocalTimes();
});