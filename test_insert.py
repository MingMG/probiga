import sys
sys.path.insert(0, '/opt/ProBigA')
from server.engine.stock_analysis_engine import StockAnalysisEngine
from sqlalchemy import create_engine, text
import os

MYSQL_URL = os.environ.get("MYSQL_URL", "mysql+pymysql://root:ProBigA%4070966@localhost:3306/probiga?charset=utf8mb4")
engine = create_engine(MYSQL_URL, pool_pre_ping=True)

analysis_engine = StockAnalysisEngine()
result = analysis_engine.analyze('600707', full_data=True)

print(f'stock_code: {result.stock_code}')
print(f'sentiment_score: {result.scores.sentiment}')
print(f'market_mood_score: {result.scores.market_mood}')
print(f'event_score: {result.scores.event}')
print(f'short_term_score: {result.short_term_score}')
print(f'recommend_status: {result.recommend.status}')

# Test the INSERT
try:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO st_recommended_stocks
            (stock_code, short_name, ai_score, long_term_score, short_term_score,
             fundamental, capital_score, valuation, technical,
             reason, sources, pick_date,
             recommend_status, recommend_reason, event_risk_level,
             sentiment_score, market_mood_score, event_score, created_at)
            VALUES (:code, :name, :score, :lt_score, :st_score,
                    :fund, :cap, :val, :tech,
                    :reason, :sources, :pick,
                    :rec_status, :rec_reason, :risk_level,
                    :sentiment, :market_mood, :event, NOW())
        """), {
            "code": "999999", "name": "TEST", "score": result.short_term_score,
            "lt_score": result.long_term_score, "st_score": result.short_term_score,
            "fund": result.scores.fundamental, "cap": result.scores.capital, "val": result.scores.valuation,
            "tech": result.scores.technical, "reason": result.summary, "sources": "test",
            "pick": "2026-06-05",
            "rec_status": result.recommend.status,
            "rec_reason": result.recommend.reason,
            "risk_level": result.event_risk.level,
            "sentiment": result.scores.sentiment,
            "market_mood": result.scores.market_mood,
            "event": result.scores.event,
        })
    print("INSERT successful")
except Exception as e:
    print(f"INSERT error: {e}")

# Verify the data was saved
with engine.connect() as conn:
    rows = conn.execute(text("SELECT stock_code, sentiment_score, market_mood_score, event_score FROM st_recommended_stocks WHERE stock_code = '999999'")).fetchall()
    for row in rows:
        print(f"Saved data: stock_code={row[0]}, sentiment={row[1]}, market_mood={row[2]}, event={row[3]}")

# Clean up
with engine.begin() as conn:
    conn.execute(text("DELETE FROM st_recommended_stocks WHERE stock_code = '999999'"))
    print("Cleaned up test data")
