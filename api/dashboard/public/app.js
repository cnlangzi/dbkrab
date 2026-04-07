/**
 * dbkrab Dashboard - Common Utilities
 */

/**
 * Convert UTC time string to local timezone
 * @param {string} timeString - Time string (UTC with Z, or with timezone offset)
 * @returns {string} Local time string in zh-CN format
 */
function formatLocalTime(timeString) {
    if (!timeString || timeString === 'null' || timeString === '-' || timeString === 'N/A' || timeString === 'Never') {
        return '-';
    }
    
    try {
        let date;
        // Check if the string already has timezone info
        if (timeString.includes('+') || timeString.includes('-', 10) || timeString.endsWith('Z')) {
            // Already has timezone info, parse directly
            date = new Date(timeString);
        } else {
            // No timezone info, assume UTC and add Z
            date = new Date(timeString + 'Z');
        }
        
        if (isNaN(date.getTime())) {
            return timeString;
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
        return timeString;
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