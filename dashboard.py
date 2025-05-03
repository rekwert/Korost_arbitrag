# dashboard.py
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt # Убедись, что matplotlib установлен
import matplotlib.colors as mcolors

# Импорты наших модулей
try:
    import config
    import data_store
    from models import TickerData
except ModuleNotFoundError:
    st.error(
        "Ошибка: Не найдены модули data_store.py, config.py или models.py."
    )
    st.stop()

# Компонент для автообновления
from streamlit_autorefresh import st_autorefresh

# --- Настройки ---
REFRESH_INTERVAL_SECONDS = 3
DECIMAL_PLACES_PRICE = 8
DECIMAL_PLACES_SIZE = 4
DECIMAL_PLACES_SPREAD = 4
DECIMAL_PLACES_VOLUME_USD = 2
TS_FRESH_THRESHOLD = 5.0
TS_STALE_THRESHOLD = 15.0

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - DASHBOARD - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Настройки Streamlit ---
st.set_page_config(
    page_title="Арбитражный Монитор", page_icon="🚀", layout="wide"
)
st.title("🚀 Арбитражный Монитор Криптовалютных Бирж (Redis)")


# --- Функции ---
def загрузить_данные_из_бд() -> dict | None:
    # ... (код без изменений) ...
    logger.info("Загрузка данных из Redis...")
    try:
        exchanges_to_load = [config.BYBIT_EXCHANGE_NAME, config.BINANCE_EXCHANGE_NAME, config.MEXC_EXCHANGE_NAME, config.KUCOIN_EXCHANGE_NAME]
        data = data_store.get_all_tickers_sync(exchanges_to_load, list(config.SYMBOLS_TO_TRACK))
        logger.info(f"Данные из Redis загружены. Ключей бирж: {len(data)}"); return data
    except Exception as e: logger.error(f"Ошибка get_all_tickers_sync: {e}", exc_info=True); st.error(f"Ошибка Redis: {e}"); return None


def рассчитать_спреды(
    данные: dict, символы: list[str], биржи: list[str], порог: Decimal
):
    # ... (код без изменений) ...
    все_спреды = {}; возможности = []
    try: порог_decimal = Decimal(str(порог))
    except: порог_decimal = Decimal('0.1')
    for символ in символы:
        все_спреды[символ] = {}; данные_по_символу = {}; валидные_биржи_для_символа = []
        for имя_биржи in биржи:
            if имя_биржи not in данные: continue
            инфо_тикера = данные.get(имя_биржи, {}).get(символ)
            try:
                if isinstance(инфо_тикера, TickerData) and инфо_тикера.bid_price and инфо_тикера.ask_price and инфо_тикера.bid_price > 0 and инфо_тикера.ask_price > 0:
                    данные_по_символу[имя_биржи] = {"bid": инфо_тикера.bid_price, "ask": инфо_тикера.ask_price, "ts": инфо_тикера.timestamp_ms}; валидные_биржи_для_символа.append(имя_биржи)
            except Exception as e: logger.warning(f"Ошибка тикера {имя_биржи}/{символ}: {e}"); continue
        if len(валидные_биржи_для_символа) < 2: continue
        for i in range(len(валидные_биржи_для_символа)):
            for j in range(i + 1, len(валидные_биржи_для_символа)):
                ex1_name = валидные_биржи_для_символа[i]; ex2_name = валидные_биржи_для_символа[j]; ticker1 = данные_по_символу[ex1_name]; ticker2 = данные_по_символу[ex2_name]
                ask1, bid1 = ticker1["ask"], ticker1["bid"]; ask2, bid2 = ticker2["ask"], ticker2["bid"]
                profit_1_2 = ((bid2 - ask1) / ask1) * 100 if ask1 > 0 else Decimal('-inf'); profit_2_1 = ((bid1 - ask2) / ask2) * 100 if ask2 > 0 else Decimal('-inf')
                key_1_2 = f"{ex1_name} -> {ex2_name}"; key_2_1 = f"{ex2_name} -> {ex1_name}"
                все_спреды[символ][key_1_2] = profit_1_2; все_спреды[символ][key_2_1] = profit_2_1
                if profit_1_2 >= порог_decimal: возможности.append({"символ": символ, "купить_биржа": ex1_name, "купить_цена": ask1, "продать_биржа": ex2_name, "продать_цена": bid2, "спред_проц": profit_1_2})
                if profit_2_1 >= порог_decimal: возможности.append({"символ": символ, "купить_биржа": ex2_name, "купить_цена": ask2, "продать_биржа": ex1_name, "продать_цена": bid1, "спред_проц": profit_2_1})
    return все_спреды, возможности

def style_time_delta(delta_sec: float | None) -> str:
    # ... (код без изменений) ...
    if delta_sec is None: return ""
    if delta_sec <= TS_FRESH_THRESHOLD: return "background-color: #90EE90;"
    if delta_sec <= TS_STALE_THRESHOLD: return "background-color: #FFFFE0;"
    return "background-color: #F08080; color: white;"

def highlight_best_prices(row):
    # ... (код без изменений) ...
    styles = [""] * len(row.index); символ = row.name; _, best_bid_ex = лучшие_биды.get(символ, (None, "")); _, best_ask_ex = лучшие_аски.get(символ, (None, ""))
    for i, col_name in enumerate(row.index):
        if best_bid_ex and f"{best_bid_ex} Предложение (Bid)" == col_name: styles[i] = "background-color: #90EE90; font-weight: bold;"
        if best_ask_ex and f"{best_ask_ex} Запрос (Ask)" == col_name: styles[i] = "background-color: #F08080; color: white; font-weight: bold;"
    return styles

# --- Основная часть Дашборда ---
актуальные_данные = загрузить_данные_из_бд()
if актуальные_данные is None:
    актуальные_данные = st.session_state.get("last_valid_db_data", None)
    if актуальные_данные: st.warning("Не удалось загрузить свежие данные. Отображаются последние доступные.")
    else: st.error("Не удалось загрузить данные."); st.stop()
else: st.session_state["last_valid_db_data"] = актуальные_данные

# --- Фильтры ---
# ... (код без изменений) ...
st.sidebar.header("Фильтры Отображения")
доступные_биржи = sorted(list(актуальные_данные.keys())); доступные_символы = sorted(config.SYMBOLS_TO_TRACK)
выбранные_символы = st.sidebar.multiselect("Символы:", options=доступные_символы, default=доступные_символы)
выбранные_биржи = st.sidebar.multiselect("Биржи:", options=доступные_биржи, default=доступные_биржи)
if not выбранные_символы: выбранные_символы = доступные_символы
if not выбранные_биржи: выбранные_биржи = доступные_биржи
show_spreads = len(выбранные_биржи) >= 2
if not show_spreads: st.sidebar.warning("Выберите минимум 2 биржи.")

# --- Расчеты ---
рассчитанные_спреды, арбитражные_ситуации = рассчитать_спреды(актуальные_данные, выбранные_символы, выбранные_биржи, config.ARBITRAGE_THRESHOLD_PCT)

# --- Метрики ---
# ... (код без изменений) ...
col1, col2, col3 = st.columns(3)
active_ex_count = sum(1 for ex in выбранные_биржи if any(актуальные_данные.get(ex,{}).get(sym) for sym in выбранные_символы))
col1.metric("Активные биржи", f"{active_ex_count} / {len(выбранные_биржи)}"); col2.metric("Отслеживаемые пары", len(выбранные_символы)); col3.metric("Арбитражные ситуации", len(арбитражные_ситуации))
st.divider()

# --- Вкладки ---
tab_prices, tab_spreads, tab_arbitrage = st.tabs(["📊 Обзор Цен и Объемов", "📈 Анализ Спредов", "💰 Арбитраж"])

# === ВКЛАДКА: ОБЗОР ЦЕН И ОБЪЕМОВ ===
with tab_prices:
    st.subheader("Текущие цены (Bid/Ask)")
    # ... (Код для первой таблицы df_цены остается БЕЗ ИЗМЕНЕНИЙ) ...
    список_цен_для_таблицы = []; price_columns = ["Символ"] + [f"{ex} {type_}" for ex in выбранные_биржи for type_ in ["Предложение (Bid)", "Запрос (Ask)"]]; лучшие_биды = {s: (Decimal("-inf"), "") for s in выбранные_символы}; лучшие_аски = {s: (Decimal("inf"), "") for s in выбранные_символы}
    for символ in выбранные_символы:
        данные_строки = {"Символ": символ}
        for имя_биржи in выбранные_биржи:
            инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ); bid_col, ask_col = f"{имя_биржи} Предложение (Bid)", f"{имя_биржи} Запрос (Ask)"; bid_val, ask_val = None, None
            if isinstance(инфо_тикера, TickerData): bid_val, ask_val = инфо_тикера.bid_price, инфо_тикера.ask_price; данные_строки[bid_col] = f"{bid_val:.{DECIMAL_PLACES_PRICE}f}" if bid_val else "N/A"; данные_строки[ask_col] = f"{ask_val:.{DECIMAL_PLACES_PRICE}f}" if ask_val else "N/A";
            else: данные_строки[bid_col], данные_строки[ask_col] = "N/A", "N/A"
            if bid_val is not None and bid_val > лучшие_биды[символ][0]: лучшие_биды[символ] = (bid_val, имя_биржи)
            if ask_val is not None and ask_val < лучшие_аски[символ][0]: лучшие_аски[символ] = (ask_val, имя_биржи)
        список_цен_для_таблицы.append(данные_строки)
    if список_цен_для_таблицы: df_цены = pd.DataFrame(список_цен_для_таблицы, columns=price_columns).set_index("Символ"); st.dataframe(df_цены.style.apply(highlight_best_prices, axis=1), use_container_width=True)
    else: st.info("Нет данных цен.")


    # --- Таблица для СУММ в USD и времени обновления ---
    st.subheader("Суммы ордеров (USD) и время обновления") # Изменили заголовок
    список_сумм_для_таблицы = [] # Новое имя списка
    стили_сумм_для_таблицы = [] # Новое имя списка
    # --- ИЗМЕНЕНИЕ 1: Определяем НОВЫЙ набор колонок ---
    volume_columns = ["Символ"] + [
        f"{ex} {type_}" for ex in выбранные_биржи
        for type_ in ["Сумма Bid (USD)", "Сумма Ask (USD)", "Обновлено (сек)"] # Только суммы и время
    ]
    # --- КОНЕЦ ИЗМЕНЕНИЯ 1 ---

    текущее_время_utc = datetime.now(timezone.utc)
    # decimal_quantizer_size больше не нужен здесь
    decimal_quantizer_usd = Decimal('1e-' + str(DECIMAL_PLACES_VOLUME_USD))

    for символ in выбранные_символы:
        данные_строки, данные_стиля_строки = {"Символ": символ}, {"Символ": ""}
        for имя_биржи in выбранные_биржи:
            инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ)
            # --- ИЗМЕНЕНИЕ 2: Колонки и переменные только для сумм и времени ---
            # bsize_col = f"{имя_биржи} Объем Bid" # Удалено
            bsum_col = f"{имя_биржи} Сумма Bid (USD)"
            # asize_col = f"{имя_биржи} Объем Ask" # Удалено
            asum_col = f"{имя_биржи} Сумма Ask (USD)"
            ts_col = f"{имя_биржи} Обновлено (сек)"
            delta_s = None
            # Заполняем стили по умолчанию только для нужных колонок
            for col in [bsum_col, asum_col, ts_col]: данные_стиля_строки[col] = ""

            if isinstance(инфо_тикера, TickerData):
                b_s, a_s = инфо_тикера.bid_size, инфо_тикера.ask_size
                b_p, a_p = инфо_тикера.bid_price, инфо_тикера.ask_price
                ts_ms = инфо_тикера.timestamp_ms
                # Расчет и запись сумм
                данные_строки[bsum_col] = str((b_s * b_p).quantize(decimal_quantizer_usd)) if b_s and b_p else "N/A"
                данные_строки[asum_col] = str((a_s * a_p).quantize(decimal_quantizer_usd)) if a_s and a_p else "N/A"
                # --- КОНЕЦ ИЗМЕНЕНИЯ 2 ---
                # Время и стили времени (без изменений)
                if ts_ms and ts_ms > 0:
                    try: delta_s = (текущее_время_utc - datetime.fromtimestamp(ts_ms / 1000, timezone.utc)).total_seconds(); данные_строки[ts_col] = f"{delta_s:.1f}"
                    except: данные_строки[ts_col] = "Ошибка"
                else: данные_строки[ts_col] = "N/A"
                данные_стиля_строки[ts_col] = style_time_delta(delta_s)
            else:
                # Заполняем N/A для сумм и времени
                данные_строки[bsum_col] = "N/A"
                данные_строки[asum_col] = "N/A"
                данные_строки[ts_col] = "N/A"

        список_сумм_для_таблицы.append(данные_строки); стили_сумм_для_таблицы.append(данные_стиля_строки)

    if список_сумм_для_таблицы:
        df_суммы = pd.DataFrame(список_сумм_для_таблицы, columns=volume_columns).set_index("Символ") # Используем новое имя DF
        # --- ИЗМЕНЕНИЕ 3: Создаем df_стили с правильными колонками ---
        style_columns = [col for col in volume_columns if col != "Символ"]
        df_стили = pd.DataFrame(стили_сумм_для_таблицы, index=df_суммы.index, columns=style_columns) # Используем новое имя DF и стилей
        # --- КОНЕЦ ИЗМЕНЕНИЯ 3 ---
        st.dataframe(df_суммы.style.apply(lambda r: df_стили.loc[r.name], axis=1), use_container_width=True) # Используем новое имя DF
    else: st.info("Нет данных объемов.")

# === ВКЛАДКА: АНАЛИЗ СПРЕДОВ ===
with tab_spreads:
    st.subheader("Таблица положительных спредов (%)")
    if show_spreads:
        список_спредов_для_таблицы = []
        ключи_пар_множество = set()
        min_spread_val = (Decimal("inf"), "", "")
        max_spread_val = (Decimal("-inf"), "", "")

        if len(выбранные_биржи) >= 2:
            for i in range(len(выбранные_биржи)):
                for j in range(i + 1, len(выбранные_биржи)):
                    ex1, ex2 = выбранные_биржи[i], выбранные_биржи[j]
                    ключи_пар_множество.add(f"{ex1} -> {ex2}"); ключи_пар_множество.add(f"{ex2} -> {ex1}")
        отсортированные_ключи_пар = sorted(list(ключи_пар_множество))

        данные_для_стилизации = [] # Переименуем для ясности
        for символ in выбранные_символы:
            if символ in рассчитанные_спреды and рассчитанные_спреды[символ]:
                данные_строки = {"Символ": символ}; спреды_символа = рассчитанные_спреды[символ]
                has_positive_spread = False
                for ключ_пары in отсортированные_ключи_пар:
                    спред_проц = спреды_символа.get(ключ_пары)
                    # Сохраняем ТОЛЬКО ПОЛОЖИТЕЛЬНЫЕ значения Decimal для стилизации
                    if спред_проц is not None and спред_проц >= 0 and спред_проц != Decimal("-inf"):
                        данные_строки[ключ_пары] = спред_проц
                        has_positive_spread = True
                        if спред_проц > max_spread_val[0]: max_spread_val = (спред_проц, символ, ключ_пары)
                    else:
                        данные_строки[ключ_пары] = None # Используем None для отрицательных/отсутствующих
                        # Ищем минимум среди всех рассчитанных (даже отрицательных)
                        if спред_проц is not None and спред_проц != Decimal("-inf"):
                            if спред_проц < min_spread_val[0]: min_spread_val = (спред_проц, символ, ключ_пары)

                # Добавляем строку только если в ней были положительные спреды
                # (можно убрать это условие, если хочешь видеть все строки, но с пустыми ячейками)
                # if has_positive_spread:
                данные_для_стилизации.append(данные_строки)

        if данные_для_стилизации:
            df_спреды_стиль = pd.DataFrame(данные_для_стилизации).set_index("Символ")

            # --- ИЗМЕНЕНИЕ: Возвращаемся к applymap ---
            def color_positive_spread_only(val):
                """Красит только положительные: >=порога=зеленый, >0=желтый/золотой."""
                color = 'white' # По умолчанию для < 0 или None
                text_color = 'black'
                try:
                    if isinstance(val, Decimal):
                         # Сначала проверяем максимум (если он положительный)
                         if val == max_spread_val[0] and val > 0:
                             color = '#FFD700' # Gold
                         # Затем проверяем порог арбитража
                         elif val >= config.ARBITRAGE_THRESHOLD_PCT:
                             color = 'lightgreen'
                         # Затем остальные положительные
                         elif val >= 0:
                             color = 'lightyellow'
                         # Отрицательные и None остаются белыми
                except Exception as e:
                    logger.error(f"Ошибка в color_positive_spread_only {val}: {e}")
                return f'background-color: {color}; color: {text_color}'

            st.dataframe(
                df_спреды_стиль.style.apply(
                    lambda col: col.map(color_positive_spread_only), # Используем applymap с новой функцией
                    subset=отсортированные_ключи_пар
                ).format(
                    "{:." + str(DECIMAL_PLACES_SPREAD) + "f}%",
                    subset=отсортированные_ключи_пар,
                    na_rep="" # Пустая строка для None (отрицательных спредов)
                ).set_properties(**{'text-align': 'center'}),
                use_container_width=True,
            )
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            # Отображение min/max спреда
            if max_spread_val[0] > Decimal("-inf"): st.caption(f"📈 Макс. +спред: {max_spread_val[1]} ({max_spread_val[2]}) = {max_spread_val[0]:.{DECIMAL_PLACES_SPREAD}f}%")
            if min_spread_val[0] < Decimal("inf"): st.caption(f"📉 Мин. -спред: {min_spread_val[1]} ({min_spread_val[2]}) = {min_spread_val[0]:.{DECIMAL_PLACES_SPREAD}f}%")

        else: st.info("Положительных спредов для выбранных фильтров не найдено.")
    else:
        st.warning("Выберите минимум 2 биржи для отображения спредов.")

# === ВКЛАДКА: АРБИТРАЖ ===
with tab_arbitrage:
    # ... (Код отображения арбитражных ситуаций БЕЗ ИЗМЕНЕНИЙ) ...
    st.subheader(f"Найденные арбитражные ситуации (спред >= {config.ARBITRAGE_THRESHOLD_PCT}%)")
    отфильтрованные_ситуации = [op for op in арбитражные_ситуации if op["символ"] in выбранные_символы and op["купить_биржа"] in выбранные_биржи and op["продать_биржа"] in выбранные_биржи]
    if отфильтрованные_ситуации:
        отфильтрованные_ситуации.sort(key=lambda x: x["спред_проц"], reverse=True)
        for ситуация in отфильтрованные_ситуации:
            цена_покупки_стр = f"{ситуация['купить_цена']:.{DECIMAL_PLACES_PRICE}f}"; цена_продажи_стр = f"{ситуация['продать_цена']:.{DECIMAL_PLACES_PRICE}f}"; спред_стр = f"{ситуация['спред_проц']:.{DECIMAL_PLACES_SPREAD}f}%"
            st.success(f"**{ситуация['символ']}**: Купить **{ситуация['купить_биржа']}** @ {цена_покупки_стр} ➡️ Продать **{ситуация['продать_биржа']}** @ {цена_продажи_стр}. **Спред: {спред_стр}**")
    else: st.info(f"Арбитражных ситуаций >= {config.ARBITRAGE_THRESHOLD_PCT}% не найдено.")

# --- Время обновления ---
st.caption(f"Дашборд обновлен: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")
st_autorefresh(interval=REFRESH_INTERVAL_SECONDS * 1000, key="обновлятор_данных")