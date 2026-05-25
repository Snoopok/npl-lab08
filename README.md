# NPL Lab08 - Транзакционная аналитика
## Архитектура
- **Батч-пайплайн**: Dagster + S3 (Yandex Cloud)
- **Хранилище**: PostgreSQL
- **Дашборд**: Streamlit
- **Оркестрация**: Dagster
## Как запустить
```bash
docker compose up -d
* Dagster UI: http://localhost:3000
* Streamlit: http://localhost:8501
Что делает пайплайн
1. Загрузка данных из S3 (каждый день):
   * Транзакции (батч каждые 10 мин)
   * Отмены (ежедневно)
   * Справочники (users, promo_codes)
2. Обработка проблем в данных:
   * Дедупликация transaction_id
   * Пустой user_id → -1 (unknown)
   * Несуществующие user_id → помечены флагом user_valid
   * Нулевые суммы → помечены флагом amount_zero
   * Просроченные промокоды → помечены флагом promo_expired
   * Унификация дат (created_at → created_at_dt)
3. Витрины:
   * fact_transactions_clean
   * fact_cancellations_clean
   * fact_hourly_stats
   * fact_daily_revenue
   * fact_promo_analysis
Базовая часть дашборда
1. Распределение транзакций по часам — столбчатая диаграмма
2. Количество покупок по часам — круговая диаграмма (purchase + completed)
3. Выручка по дням — линейный график
4. Анализ промокодов — таблица (usage vs limits)
Мотивация решений
* S3 + Dagster: Простота, идемпотентность, легко масштабировать
* PostgreSQL: Универсальное хранилище, поддержка SQL
* Streamlit: Быстрый дашборд, интерактивность
* Обработка edge cases: Пайплайн не падает, данные помечены флагами
Автор
Антон Мартемьянов
Сдача
Репозиторий: https://github.com/Snoopok/npl-lab08
