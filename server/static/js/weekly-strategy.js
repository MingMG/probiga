(function () {
    "use strict";

    const palettes = ["#2563eb", "#b45309", "#0e7490", "#7c3aed", "#be123c", "#15803d"];
    const missing = "—";
    const percent = value => Number.isFinite(value) ? value.toFixed(2) + "%" : missing;
    const share = value => Number.isFinite(value) ? percent(value * 100) : missing;
    const amount = (value, unit) => Number.isFinite(value) ? (value / unit).toFixed(2) : missing;
    const count = value => Number.isInteger(value) ? String(value) : missing;

    function saveSnapshot(report, etf) {
        const snapshot = etf ? {weekly_market: report, broad_etf: etf} : report;
        const file = new Blob([JSON.stringify(snapshot, null, 2)], {type: "application/json"});
        const url = URL.createObjectURL(file);
        const link = document.createElement("a");
        link.href = url;
        link.download = "周度策略-" + report.week_start + "-" + report.snapshot_sha256.slice(0, 8) + ".json";
        link.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }

    function drawPath(canvas, paths) {
        const usable = paths.filter(path => path.points.some(point => point.normalized !== null));
        const points = usable.flatMap(path => path.points.filter(point => point.normalized !== null));
        if (!points.length) {
            canvas.hidden = true;
            return;
        }
        const ctx = canvas.getContext("2d");
        const values = points.map(point => point.normalized);
        const low = Math.floor(Math.min(...values, 99));
        const high = Math.ceil(Math.max(...values, 101));
        const dates = paths[0].points.map(point => point.date);
        const left = 42;
        const right = canvas.width - 26;
        const top = 18;
        const bottom = canvas.height - 34;
        const y = value => bottom - (value - low) / (high - low) * (bottom - top);
        const x = index => left + index * (right - left) / Math.max(1, dates.length - 1);
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.font = "13px system-ui";
        ctx.fillStyle = "#354258";
        for (const value of [low, 100, high]) {
            ctx.strokeStyle = "#ccd6e1";
            ctx.beginPath();
            ctx.moveTo(left, y(value));
            ctx.lineTo(right, y(value));
            ctx.stroke();
            ctx.fillText(String(value), 5, y(value) + 4);
        }
        dates.forEach((date, index) => ctx.fillText(date.slice(5), x(index) - 18, bottom + 23));
        paths.forEach((path, index) => {
            ctx.strokeStyle = palettes[index];
            ctx.fillStyle = palettes[index];
            ctx.lineWidth = 2;
            let previous = null;
            path.points.forEach((point, pointIndex) => {
                if (point.normalized === null) {
                    previous = null;
                    return;
                }
                const position = [x(pointIndex), y(point.normalized)];
                if (previous) {
                    ctx.beginPath();
                    ctx.moveTo(...previous);
                    ctx.lineTo(...position);
                    ctx.stroke();
                }
                ctx.beginPath();
                ctx.arc(...position, 4, 0, 2 * Math.PI);
                ctx.fill();
                previous = position;
            });
        });
    }

    window.renderWeeklyStrategy = function (host, get, escape) {
        host.innerHTML = '<section class="weekly-strategy"><header><h2>周度策略</h2>' +
            '<form class="search-bar"><label>查看日期 <input type="date" name="week_of"></label>' +
            '<button type="submit">查看所在周</button></form></header>' +
            '<div data-weekly-content aria-live="polite"></div></section>';
        const content = host.querySelector("[data-weekly-content]");
        const form = host.querySelector("form");
        let requestId = 0;
        function table(headers, rows) {
            return '<div class="weekly-scroll"><table><thead><tr>' +
                headers.map(value => "<th>" + escape(value) + "</th>").join("") +
                "</tr></thead><tbody>" + rows.map(row => "<tr>" + row.map(value =>
                    "<td>" + escape(value ?? missing) + "</td>").join("") + "</tr>").join("") +
                "</tbody></table></div>";
        }
        function display(report) {
            const cov = report.coverage;
            const turns = report.higher_turnover_down_days.map(turn =>
                turn.date + " 上证指数 " + percent(turn.index_return_pct) + "，全样本成交环比 " +
                percent(turn.amount_change_pct));
            const sectors = report.days.map(day => {
                const list = Object.entries(day.industries).sort((a, b) => b[1].median_return_pct - a[1].median_return_pct);
                return "<details><summary>" + escape(day.date) + " · " +
                    (list.length ? list.length + " 个行业" : "数据不足") + "</summary>" +
                    (list.length ? table(["行业", "有效样本", "收益中位数", "上涨占比", "成交占比"], list.map(([name, row]) =>
                        [name, count(row.sample_count), percent(row.median_return_pct), share(row.up_share), share(row.amount_share)])) :
                        "<p>" + escape(day.industry_reason || "暂无通过校验的行业快照") + "</p>") + "</details>";
            }).join("");
            content.innerHTML = "<h3>" + escape(report.week_start) + " 至 " + escape(report.week_end) + "</h3>" +
                "<p>历史重建 · 已校验日度复盘 " + cov.ready_reviews + "/" + cov.sessions +
                " · 成交 " + cov.turnover_days + "/" + cov.sessions + " · 行业 " + cov.industry_days +
                "/" + cov.sessions + " · 融资余额 " + cov.margin_days + "/" + cov.sessions + "</p>" +
                (Object.keys(report.source_errors).length ? "<p>读取失败：" + escape(Object.keys(report.source_errors).join("、")) + "</p>" : "") +
                "<button type='button' data-save-weekly>下载数据快照</button>" +
                "<h3>指数收盘路径</h3><p>以 " + escape(report.baseline_date || "暂无基准日") + " 收盘为 100。</p>" +
                '<canvas width="790" height="255" role="img" aria-label="指数归一化收盘走势"></canvas>' +
                '<div class="weekly-legend">' + report.index_paths.map((path, i) =>
                    '<span style="--series:' + palettes[i] + '">' + escape(path.name) + "</span>").join("") + "</div>" +
                table(["指数", "周涨跌", "缺行情日期"], report.index_paths.map(path =>
                    [path.name, percent(path.week_return_pct), path.missing_dates.join("、") || missing])) +
                "<details><summary>每日指数收盘</summary>" + table(["日期", ...report.index_paths.map(path => path.name)],
                    report.index_paths[0].points.map((point, i) =>
                        [point.date, ...report.index_paths.map(path => amount(path.points[i].close, 1))])) + "</details>" +
                "<h3>量价与市场广度</h3><p>全样本日均成交 " + amount(report.turnover.daily_mean_cny, 1e12) +
                " 万亿元 · 较前周 " + percent(report.turnover.change_pct) + "</p>" +
                (turns.length ? "<p>放量下跌日：" + escape(turns.join("；")) + "</p>" : "") +
                table(["交易日", "成交（万亿元）", "成交环比", "上涨", "下跌", "平盘", "收益中位数", "TMT 成交占比", "融资余额（亿元）", "成交证据", "复盘数据截止"],
                    report.days.map(day => [day.date, amount(day.amount_cny, 1e12), percent(day.amount_change_pct),
                        count(day.breadth.up_count), count(day.breadth.down_count), count(day.breadth.flat_count),
                        percent(day.median_return_pct), share(day.tmt_amount_share), amount(day.margin_balance_cny, 1e8),
                        day.amount_evidence_date ? day.amount_evidence_date + " 复盘" : missing,
                        day.data_cutoff_at?.replace("T", " ")])) +
                "<h3>申万一级行业</h3><p>TMT 为电子、通信、计算机。</p>" + sectors +
                "<h3>宽基 ETF 份额</h3><div data-weekly-etf></div>";
            const etfHost = content.querySelector("[data-weekly-etf]");
            const finalDay = report.days.at(-1)?.date;
            let etfSnapshot = null;
            content.querySelector("[data-save-weekly]").addEventListener("click", () => saveSnapshot(report, etfSnapshot));
            drawPath(content.querySelector("canvas"), report.index_paths);
            if (!finalDay) {
                etfHost.textContent = "本周无交易日。";
                return;
            }
            etfHost.textContent = "读取交易所份额…";
            get("/broad-etf-flow?trade_date=" + encodeURIComponent(finalDay) + "&days=9", 45000).then(data => {
                if (!content.contains(etfHost)) return;
                if (data.status === "error" || !data.history?.length) {
                    etfHost.textContent = "这周暂无法取得可核对的宽基份额。";
                    return;
                }
                const dates = new Set(report.days.map(day => day.date));
                const history = data.history.filter(row => dates.has(row.trade_date));
                const byDate = new Map(history.map(row => [row.trade_date, row]));
                const expected = data.coverage?.expected;
                const complete = Number.isInteger(expected) && expected > 0 &&
                    report.days.every(day => {
                        const row = byDate.get(day.date);
                        return row?.available === expected && Number.isFinite(row.net_amount);
                    });
                const total = complete ? history.reduce((sum, row) => sum + row.net_amount, 0) : null;
                etfSnapshot = {
                    scope_label: data.scope_label, data_as_of: history.at(-1)?.trade_date,
                    source_status: data.status, expected_products: expected, history,
                    weekly_net_amount_cny: total, evidence: data.flow_evidence, sources: data.sources,
                };
                etfHost.innerHTML = "<p>" + escape(data.scope_label || "核心宽基观察池") +
                    " · 份额变化 × 前一交易日收盘价估算 · 账户身份未知</p>" +
                    "<p>观察池本周合计（亿元） " + amount(total, 1e8) +
                    " · 最新有效日 " + escape(etfSnapshot.data_as_of || missing) + "</p>" +
                    table(["交易日", "净申赎估算（亿元）", "有效产品", "预期产品"], report.days.map(day => {
                        const row = byDate.get(day.date);
                        return [day.date, amount(row?.net_amount, 1e8), count(row?.available), count(expected)];
                    })) + history.map(row => {
                        if (!row.components?.length) return "";
                        return "<details><summary>" + escape(row.trade_date) + " · 产品明细</summary>" +
                            table(["代码", "名称", "前日份额", "当日份额", "估算价格", "净申赎（亿元）", "质量"],
                                row.components.map(item => [item.etf_code, item.short_name, amount(item.previous_fund_share, 1),
                                    amount(item.fund_share, 1), Number.isFinite(item.reference_price) ? item.reference_price.toFixed(6) : missing,
                                    amount(item.net_amount, 1e8), item.quality_status])) + "</details>";
                    }).join("");
            }).catch(() => {
                if (content.contains(etfHost)) etfHost.textContent = "宽基份额读取失败。";
            });
        }
        function load(value) {
            const id = ++requestId;
            content.textContent = "读取周度数据…";
            const query = value ? "?week_of=" + encodeURIComponent(value) : "";
            return get("/weekly-strategy" + query, 15000).then(report => {
                if (id !== requestId || !host.contains(content)) return;
                display(report);
                form.elements.week_of.value = report.week_start;
            }).catch(error => {
                if (id === requestId && host.contains(content)) {
                    content.textContent = "周度数据暂不可用。" + (error.message || "");
                }
            });
        }
        form.addEventListener("submit", event => {
            event.preventDefault();
            load(form.elements.week_of.value);
        });
        return load("");
    };
}());
