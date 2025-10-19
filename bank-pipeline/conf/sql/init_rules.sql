-- Minimal schema; optional seed rows. You can paste the Tesla/ChargePoint rules here as needed.
CREATE TABLE IF NOT EXISTS rules (id TEXT PRIMARY KEY, priority INTEGER, logic TEXT, stop_after_match BOOLEAN);
CREATE TABLE IF NOT EXISTS predicates (id TEXT, idx INTEGER, column TEXT, op TEXT, value TEXT, values_json TEXT, PRIMARY KEY (id, idx));
CREATE TABLE IF NOT EXISTS actions (id TEXT, idx INTEGER, target TEXT, set_value TEXT, PRIMARY KEY (id, idx));
CREATE TABLE IF NOT EXISTS rule_profiles (id TEXT, profile TEXT, PRIMARY KEY (id, profile));
