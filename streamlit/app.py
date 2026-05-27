import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="NPL Lab08 Dashboard", layout="wide")
st.title("📊 NPL Lab08 - Transactions Dashboard")
st.markdown("**Данные за последние 2 недели (14 дней)** | Базовая валюта: **TGRK**")

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
col2.metric("Total Revenue (TGRK)", f"{filtered['amount'].sum():,.0f}")
col3.metric("Unique Users", filtered['user_id'].nunique())
col4.metric("Promo Usage", filtered['promo_code_id'].notna().sum())

# === 1. РАСПРЕДЕЛЕНИЕ ПО ЧАСАМ ===
st.subheader("1. Распределение транзакций по часам")
st.caption("Ось X — час суток (0 = 00:00–01:00, 1 = 01:00–02:00 и т.д.). Ось Y — количество транзакций в этот час.")
fig1 = px.bar(hourly, x='hour', y='transaction_count', 
              title="Transactions by Hour",
              labels={'hour': 'Час суток', 'transaction_count': 'Количество транзакций'})
st.plotly_chart(fig1, use_container_width='stretch')

# === 2. КОЛИЧЕСТВО ПОКУПОК ПО ЧАСАМ ===
st.subheader("2. Количество покупок по часам (purchase + completed)")
st.caption("Круговая диаграмма показывает распределение покупок по часам суток. Размер сектора = доля покупок в этот час.")
purchase_df = filtered[filtered['transaction_type'] == 'purchase'].copy()
purchase_df['hour'] = purchase_df['created_at_dt'].dt.hour
fig2 = px.pie(purchase_df, names='hour', 
              title="Purchases by Hour",
              labels={'hour': 'Час'})
st.plotly_chart(fig2, use_container_width='stretch')

# === 3. ВЫРУЧКА ПО ДНЯМ ===
st.subheader("3. Выручка по дням (базовая валюта TGRK)")
st.caption("Ось X — дата. Ось Y — общая выручка за день в базовой валюте TGRK (конвертация через курсы валют).")
fig3 = px.line(daily, x='date', y='total_amount', 
               title="Daily Revenue",
               labels={'date': 'Дата', 'total_amount': 'Выручка (TGRK)'})
st.plotly_chart(fig3, use_container_width='stretch')

# === 4. АНАЛИЗ ПРОМОКОДОВ ===
st.subheader("4. Анализ промокодов (usage vs limits)")
st.caption("usage_count = сколько раз использовали промокод. total_amount = сумма транзакций с этим промокодом.")
if not promo.empty:
    st.dataframe(promo[['promo_code_id', 'usage_count', 'total_amount']], use_container_width='stretch')
else:
    st.info("Промокоды не использовались в последние 2 недели")
st.markdown("---")
st.caption("Данные обновляются автоматически через Dagster (scheduled job каждые 10 минут)")
