WITH first_launch AS (
    SELECT 
        userId,
        MIN(time) AS first_launch_time
    FROM users
    WHERE 
        eventName = 'launch'
        AND time >= '2024-03-01'
    GROUP BY userId
),
cohorts AS (
    SELECT 
        userId,
        DATE_TRUNC('week', first_launch_time) AS week,
        first_launch_time
    FROM first_launch
),
update_flags AS (
    SELECT 
        c.userId,
        c.week,
        SIGN(SUM(
            CASE WHEN e.eventName = 'update' 
                 AND e.time BETWEEN c.first_launch_time 
                               AND c.first_launch_time + INTERVAL '14 days' 
            THEN 1 ELSE 0 END
        )) AS has_updated
    FROM cohorts c
    LEFT JOIN users e 
        ON e.userId = c.userId
    WHERE e.eventName = 'update'
    GROUP BY c.userId, c.week, c.first_launch_time
)
SELECT 
    week,
    COUNT(userId) AS users,
    SUM(has_updated) / COUNT(userId) AS CR
FROM update_flags
GROUP BY week
ORDER BY week;