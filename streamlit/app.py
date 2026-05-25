import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="NPL Lab08 Dashboard", layout="wide")
st.title("📊 NPL Lab08 - Transactions Dashboard")

PG_URL = "postgresql://dagster:dagster@postgres:5432/dagster"
engine = create_engine(PG_URL)

@st.cache_data(ttl=60)
def load_data():
    hourly = pd.read_sql("SELECT * FROM fact_hourly_stats", engine)
    daily = pd.read_sql("SELECT * FROM fact_daily_revenue", engine)
    promo = pd.read_sql("SELECT * FROM fact_promo_analysis", engine)
    transactions = pd.read_sql("SELECT * FROM fact_transactions_clean LIMIT 5000", engine)
    return hourly, daily, promo, transactions

hourly, daily, promo, transactions = load_data()

# === SIDEBAR ===
st.sidebar.header("Фильтры")
min_amount = st.sidebar.number_input("Min Amount", value=0.0)
max_amount = st.sidebar.number_input("Max Amount", value=10000.0)

filtered = transactions[
    (transactions['amount'] >= min_amount) & 
    (transactions['amount'] <= max_amount)
]

# === METRICS ===
col1, col2, col3, col4 = st.columns(4)
col1.metric("Transactions (clean)", len(filtered))
col2.metric("Total Revenue", f"{filtered['amount'].sum():,.0f}")
col3.metric("Unique Users", filtered['user_id'].nunique())
col4.metric("Promo Usage", filtered['promo_code_id'].notna().sum())

# === 1. РАСПРЕДЕЛЕНИЕ ПО ЧАСАМ ===
st.subheader("1. Распределение транзакций по часам")
fig1 = px.bar(hourly, x='hour', y='transaction_count', title="Transactions by Hour")
st.plotly_chart(fig1, use_container_width=True)

# === 2. КОЛИЧЕСТВО ПОКУПОК ПО ЧАСАМ ===
st.subheader("2. Количество покупок по часам (purchase + completed)")
purchase_df = filtered[filtered['transaction_type'] == 'purchase'].copy()
purchase_df['hour'] = purchase_df['created_at_dt'].dt.hour
fig2 = px.pie(purchase_df, names='hour', title="Purchases by Hour")
st.plotly_chart(fig2, use_container_width=True)

# === 3. ВЫРУЧКА ПО ДНЯМ ===
st.subheader("3. Выручка по дням (базовая валюта)")
fig3 = px.line(daily, x='date', y='total_amount', title="Daily Revenue")
st.plotly_chart(fig3, use_container_width=True)

# === 4. АНАЛИЗ ПРОМОКОДОВ ===
st.subheader("4. Анализ промокодов (usage vs limits)")
st.dataframe(promo[['promo_code_id', 'usage_count', 'total_amount']], use_container_width=True)

# === RAW DATA ===
st.subheader("Raw Transactions (sample)")
st.dataframe(filtered.head(100), use_container_width=True)
