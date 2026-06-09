-- ============================================================
-- 模拟交易系统表
-- 包含：模拟持仓表、操作流水表、策略每日快照表
-- 注意：此SQL兼容MySQL 5.5+
-- ============================================================

-- 模拟交易持仓表
CREATE TABLE IF NOT EXISTS `st_sim_position` (
    `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
    `stock_code`      VARCHAR(10)  NOT NULL COMMENT '股票代码',
    `short_name`      VARCHAR(20)  DEFAULT '' COMMENT '股票名称',
    `strategy_type`   VARCHAR(20)  NOT NULL COMMENT '策略类型: ultra_short / short_term / swing',
    `buy_price`       DECIMAL(12,4) NOT NULL COMMENT '买入价格',
    `buy_amount`      DECIMAL(14,2) NOT NULL COMMENT '买入金额(元)',
    `buy_shares`      INT          NOT NULL COMMENT '买入股数',
    `buy_date`        DATE         NOT NULL COMMENT '买入日期',
    `buy_time`        VARCHAR(20)  DEFAULT '' COMMENT '买入时间(HH:MM)',
    `buy_reason`      TEXT         COMMENT '买入原因(JSON: 评分+信号)',
    `ai_score`        DECIMAL(5,2) DEFAULT 0 COMMENT '买入时AI综合评分',
    `short_score`     DECIMAL(5,2) DEFAULT 0 COMMENT '买入时短期评分',
    `long_score`      DECIMAL(5,2) DEFAULT 0 COMMENT '买入时长期评分',
    `capital_score`   DECIMAL(5,2) DEFAULT 0 COMMENT '买入时资金面评分',
    `technical_score` DECIMAL(5,2) DEFAULT 0 COMMENT '买入时技术面评分',
    `fundamental_score` DECIMAL(5,2) DEFAULT 0 COMMENT '买入时基本面评分',
    `event_risk_level` VARCHAR(10) DEFAULT 'LOW' COMMENT '买入时事件风险等级',
    `status`          VARCHAR(20)  DEFAULT 'holding' COMMENT '状态: holding / sold',
    `sell_price`      DECIMAL(12,4) DEFAULT NULL COMMENT '卖出价格',
    `sell_date`       DATE         DEFAULT NULL COMMENT '卖出日期',
    `sell_time`       VARCHAR(20)  DEFAULT '' COMMENT '卖出时间(HH:MM)',
    `sell_reason`     VARCHAR(100) DEFAULT '' COMMENT '卖出原因: take_profit / stop_loss / time_limit / trailing_stop',
    `profit`          DECIMAL(14,2) DEFAULT 0 COMMENT '盈亏金额(扣手续费后)',
    `profit_rate`     DECIMAL(8,4) DEFAULT 0 COMMENT '盈亏比例(%)',
    `holding_days`    INT          DEFAULT 0 COMMENT '持仓天数',
    `fee_total`       DECIMAL(10,2) DEFAULT 0 COMMENT '买卖手续费合计',
    `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
    `updated_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX `idx_strategy_status` (`strategy_type`, `status`),
    INDEX `idx_stock_code` (`stock_code`),
    INDEX `idx_buy_date` (`buy_date`),
    INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='模拟交易持仓表';


-- 操作流水表(模拟交易 + 自选股操作统一记录)
CREATE TABLE IF NOT EXISTS `st_trade_flow` (
    `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
    `stock_code`      VARCHAR(10)  NOT NULL COMMENT '股票代码',
    `short_name`      VARCHAR(20)  DEFAULT '' COMMENT '股票名称',
    `flow_type`       VARCHAR(20)  NOT NULL COMMENT '流水类型: sim_buy / sim_sell / watch_buy / watch_sell',
    `source`          VARCHAR(20)  NOT NULL COMMENT '来源: simulation / watchlist',
    `strategy_type`   VARCHAR(20)  DEFAULT '' COMMENT '策略类型(模拟交易时有值)',
    `trans_type`      VARCHAR(10)  NOT NULL COMMENT 'buy / sell',
    `price`           DECIMAL(12,4) NOT NULL COMMENT '交易价格',
    `shares`          INT          NOT NULL COMMENT '交易股数',
    `amount`          DECIMAL(14,2) NOT NULL COMMENT '交易金额',
    `fee`             DECIMAL(10,2) DEFAULT 0 COMMENT '手续费',
    `reason`          VARCHAR(200) DEFAULT '' COMMENT '操作原因',
    `ai_score`        DECIMAL(5,2) DEFAULT 0 COMMENT '当时AI评分',
    `trans_date`      DATE         NOT NULL COMMENT '交易日期',
    `trans_time`      VARCHAR(20)  DEFAULT '' COMMENT '交易时间(HH:MM)',
    `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_flow_type` (`flow_type`),
    INDEX `idx_source` (`source`),
    INDEX `idx_stock_date` (`stock_code`, `trans_date`),
    INDEX `idx_trans_date` (`trans_date`),
    INDEX `idx_strategy` (`strategy_type`, `trans_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='操作流水表(模拟+自选股)';


-- 策略每日快照表
CREATE TABLE IF NOT EXISTS `st_strategy_snapshot` (
    `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
    `snapshot_date`   DATE         NOT NULL COMMENT '快照日期',
    `strategy_type`   VARCHAR(20)  NOT NULL COMMENT '策略类型',
    `total_trades`    INT          DEFAULT 0 COMMENT '总交易次数(已平仓)',
    `win_count`       INT          DEFAULT 0 COMMENT '盈利次数',
    `lose_count`      INT          DEFAULT 0 COMMENT '亏损次数',
    `win_rate`        DECIMAL(6,2) DEFAULT 0 COMMENT '胜率(%)',
    `total_profit`    DECIMAL(14,2) DEFAULT 0 COMMENT '累计盈亏(元)',
    `total_fee`       DECIMAL(10,2) DEFAULT 0 COMMENT '累计手续费(元)',
    `avg_profit_rate` DECIMAL(8,4) DEFAULT 0 COMMENT '平均收益率(%)',
    `max_profit_rate` DECIMAL(8,4) DEFAULT 0 COMMENT '最大单笔收益率(%)',
    `max_loss_rate`   DECIMAL(8,4) DEFAULT 0 COMMENT '最大单笔亏损率(%)',
    `holding_count`   INT          DEFAULT 0 COMMENT '当前持仓数',
    `holding_amount`  DECIMAL(14,2) DEFAULT 0 COMMENT '当前持仓金额',
    `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_date_strategy` (`snapshot_date`, `strategy_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='策略每日快照';
