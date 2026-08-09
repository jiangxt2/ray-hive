CREATE DATABASE IF NOT EXISTS ray_hive_it;

DROP VIEW IF EXISTS ray_hive_it.primitive_view;
DROP TABLE IF EXISTS ray_hive_it.primitive_values;
DROP TABLE IF EXISTS ray_hive_it.char_values;
DROP TABLE IF EXISTS ray_hive_it.external_values;

CREATE TABLE ray_hive_it.primitive_values (
  id INT,
  enabled BOOLEAN,
  label STRING,
  amount DECIMAL(18, 4),
  event_date DATE,
  event_time TIMESTAMP,
  payload BINARY
)
STORED AS ORC;

INSERT INTO ray_hive_it.primitive_values VALUES
  (1, true, 'alpha', 12.3000, DATE '2026-01-01', TIMESTAMP '2026-01-01 01:02:03.123456789', CAST('a' AS BINARY)),
  (2, false, 'beta', -1.2500, DATE '2026-02-02', TIMESTAMP '2026-02-02 02:03:04.000000001', CAST('bb' AS BINARY)),
  (3, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, true, '', 0.0000, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', CAST('' AS BINARY)),
  (5, false, 'omega', 99999999999999.9999, DATE '2030-12-31', TIMESTAMP '2030-12-31 23:59:59.999999999', CAST('xyz' AS BINARY));

CREATE TABLE ray_hive_it.char_values (
  id INT,
  fixed_label CHAR(5)
)
STORED AS ORC;

INSERT INTO ray_hive_it.char_values VALUES
  (1, CAST('xy' AS CHAR(5)));

CREATE EXTERNAL TABLE ray_hive_it.external_values (id INT, label STRING)
STORED AS TEXTFILE
LOCATION 'file:///opt/hive/data/warehouse/ray-hive-external';

INSERT INTO ray_hive_it.external_values VALUES (10, 'server-only'), (20, 'location');

CREATE VIEW ray_hive_it.primitive_view AS
SELECT id, label FROM ray_hive_it.primitive_values WHERE id >= 2;
