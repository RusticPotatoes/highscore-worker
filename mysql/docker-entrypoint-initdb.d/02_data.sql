USE playerdata;

-- Insert data into the Players table
DELIMITER $$

CREATE PROCEDURE InsertRandomPlayers(IN NUM INT, IN possible_ban BOOL, IN confirmed_ban BOOL, IN confirmed_player BOOL)
BEGIN
    DECLARE i INT DEFAULT 1;

    WHILE i <= NUM DO
        INSERT INTO Players (
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
            UUID() AS name, -- updated later
            NOW() AS created_at, -- updated later
            NOW() AS updated_at, -- updated later
            possible_ban,
            confirmed_ban,
            confirmed_player,
            0 AS label_id,
            ROUND(RAND() * 1) AS label_jagex, -- doesn't matter?
            null AS ironman,
            null AS hardcore_ironman,
            null AS ultimate_ironman,
            UUID() AS normalized_name -- updated later
        FROM dual;

        SET i = i + 1;
    END WHILE;
END $$

DELIMITER ;


call InsertRandomPlayers(100, 1,0,0);
call InsertRandomPlayers(100, 1,1,0);
call InsertRandomPlayers(100, 0,0,1);

UPDATE Players
SET
    name = CONCAT('player', id),
    normalized_name = CONCAT('player', id)
;
