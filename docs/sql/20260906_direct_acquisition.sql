-- One-time primary-database compatibility migration for direct acquisition.
-- Stop the old writers for these tables before applying. No users, roles,
-- grants, permission tables, triggers, or validation records are created.

ALTER TABLE `sm_etf_kline`
  MODIFY COLUMN `validation_source` VARCHAR(32) NULL DEFAULT NULL,
  MODIFY COLUMN `validation_status` VARCHAR(16) NULL DEFAULT NULL,
  MODIFY COLUMN `validation_checked_at` DATETIME NULL DEFAULT NULL,
  MODIFY COLUMN `quality_status` VARCHAR(16) NULL DEFAULT NULL,
  MODIFY COLUMN `permission_status` VARCHAR(16) NULL DEFAULT NULL;

ALTER TABLE `si_etf_code`
  MODIFY COLUMN `validation_source` VARCHAR(32) NULL DEFAULT NULL,
  MODIFY COLUMN `sync_status` VARCHAR(16) NULL DEFAULT NULL;

ALTER TABLE `si_stock_finance`
  ADD COLUMN `source_update_date` VARCHAR(64) NULL;

ALTER TABLE `st_a_list_daily`
  ADD COLUMN `trade_id` VARCHAR(32) NULL;

ALTER TABLE `st_a_list_info`
  ADD COLUMN `trade_id` VARCHAR(32) NULL,
  ADD COLUMN `report_side` VARCHAR(4) NULL;
