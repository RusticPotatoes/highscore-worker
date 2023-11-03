USE playerdata;

-- Insert data into the Players table

INSERT INTO
    Players (
        name,
        created_at,
        updated_at,
        possible_ban,
        confirmed_ban,
        confirmed_player,
        label_id,
        label_jagex,
        ironman,
        hardcore_ironman,
        ultimate_ironman,
        normalized_name
    )
SELECT
    CONCAT('Player', id) AS name,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS created_at,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS updated_at,
    1 AS possible_ban,
    0 AS confirmed_ban,
    0 AS confirmed_player,
    0 AS label_id,
    ROUND(RAND() * 1) AS label_jagex,
    -- Random label_jagex between 0 and 2 (inclusive)
    null ironman,
    null AS hardcore_ironman,
    null AS ultimate_ironman,
    CONCAT('player', id) AS normalized_name
FROM (
        SELECT (a.N + b.N * 10) AS id
        FROM (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS a, (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS b
    ) AS numbers
union
SELECT
    CONCAT('Player', id) AS name,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS created_at,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS updated_at,
    1 AS possible_ban,
    -- 50% chance of possible_ban being true
    1 AS confirmed_ban,
    -- 30% chance of confirmed_ban being true, with possible_ban and label_jagex=2
    0 AS confirmed_player,
    -- 80% chance of confirmed_player being true
    0 AS label_id,
    -- Random label_id between 0 and 2 (inclusive)
    2 AS label_jagex,
    -- Random label_jagex between 0 and 2 (inclusive)
    null ironman,
    null AS hardcore_ironman,
    null AS ultimate_ironman,
    CONCAT('player', id) AS normalized_name
FROM (
        SELECT (a.N + b.N * 10 + 100) AS id
        FROM (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS a, (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS b
    ) AS numbers
union
SELECT
    CONCAT('Player', id) AS name,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS created_at,
    NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS updated_at,
    0 AS possible_ban,
    -- 50% chance of possible_ban being true
    0 AS confirmed_ban,
    -- 30% chance of confirmed_ban being true, with possible_ban and label_jagex=2
    1 AS confirmed_player,
    -- 80% chance of confirmed_player being true
    0 AS label_id,
    -- Random label_id between 0 and 2 (inclusive)
    0 AS label_jagex,
    -- Random label_jagex between 0 and 2 (inclusive)
    null ironman,
    null AS hardcore_ironman,
    null AS ultimate_ironman,
    CONCAT('player', id) AS normalized_name
FROM (
        SELECT (a.N + b.N * 10 + 200) AS id
        FROM (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS a, (
                SELECT 0 AS N
                UNION
                SELECT 1
                UNION
                SELECT 2
                UNION
                SELECT 3
                UNION
                SELECT 4
                UNION
                SELECT 5
                UNION
                SELECT 6
                UNION
                SELECT 7
                UNION
                SELECT 8
                UNION
                SELECT
                    9
            ) AS b
    ) AS numbers;
