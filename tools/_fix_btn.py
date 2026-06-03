# Fix: replace broken button onclick="window.open(minuteBtn..." with minuteBtn(r.stock_code)
p = r"e:\My Code\ProBigA\server\static\js\app.js"
with open(p, "r", encoding="utf-8") as f:
    c = f.read()

# The broken pattern starts with: <td><button onclick="window.open(minuteBtn(' and ends with 📈</button></td>
# We need to replace all of these with: <td>' + minuteBtn(r.stock_code) + '</td>

import re

# Find all occurrences of the broken pattern
# Pattern: <td><button onclick="window.open(minuteBtn(...)...">📈</button></td>
pattern = re.compile(r"<td><button onclick=\"window\.open\(minuteBtn\('[^']*',[^)]*\)[^>]*>.*?</button></td>")
matches = pattern.findall(c)
print(f"Found {len(matches)} broken button patterns")

# Replace each one
replacement = "<td>' + minuteBtn(r.stock_code) + '</td>"
c2 = pattern.sub(replacement, c)

with open(p, "w", encoding="utf-8") as f:
    f.write(c2)
print("Fixed!")
