# dashboard.py
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

import numpy as np
import pandas as pd
import streamlit as st
# import matplotlib.pyplot as plt # Не нужен для applymap
# import matplotlib.colors as mcolors # Не нужен для applymap

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
DECIMAL_PLACES_PRICE = 5  # Уменьшенная точность цен
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
# @st.cache_data # Отключаем кэширование
def загрузить_данные_из_бд() -> dict | None:
    """Синхронно загружает все тикеры из Redis."""
    logger.info("Загрузка данных из Redis...")
    try:
        exchanges_to_load = [
            config.BYBIT_EXCHANGE_NAME, config.BINANCE_EXCHANGE_NAME,
            config.MEXC_EXCHANGE_NAME, config.KUCOIN_EXCHANGE_NAME,
        ]
        data = data_store.get_all_tickers_sync(
            exchanges_to_load, list(config.SYMBOLS_TO_TRACK)
        )
        logger.info(f"Данные из Redis загружены. Ключей бирж: {len(data)}")
        return data
    except Exception as e:
        logger.error(f"Ошибка get_all_tickers_sync: {e}", exc_info=True)
        st.error(f"Ошибка Redis: {e}")
        return None

def рассчитать_спреды( # Оставляем старое название, но меняем возвращаемое значение
    данные: dict, символы: list[str], биржи: list[str], порог: Decimal
):
    """
    Находит ЛУЧШИЙ положительный спред и все арбитражные возможности для каждой пары.
    Возвращает:
        - best_positive_spreads (dict): {символ: {"пара": "Биржа1 -> Биржа2", "спред_проц": Decimal}}
        - opportunities (list): Список арбитражных ситуаций >= порога.
    """
    # все_спреды больше не нужен
    best_positive_spreads = {} # Словарь для лучшего положительного спреда по каждому символу
    возможности = []
    try:
        порог_decimal = Decimal(str(порог))
    except (InvalidOperation, TypeError):
        порог_decimal = Decimal("0.1")

    for символ in символы:
        best_positive_spreads[символ] = {"пара": "N/A", "спред_проц": Decimal("-inf")} # Инициализация
        данные_по_символу = {}
        валидные_биржи_для_символа = []

        # ... (код сбора валидных данных по символу без изменений) ...
        for имя_биржи in биржи:
             if имя_биржи not in данные: continue
             инфо_тикера = данные.get(имя_биржи, {}).get(символ)
             try:
                 if (isinstance(инфо_тикера, TickerData) and инфо_тикера.bid_price and инфо_тикера.ask_price and инфо_тикера.bid_price > 0 and инфо_тикера.ask_price > 0):
                     данные_по_символу[имя_биржи] = {"bid": инфо_тикера.bid_price, "ask": инфо_тикера.ask_price, "ts": инфо_тикера.timestamp_ms}; валидные_биржи_для_символа.append(имя_биржи)
             except Exception as e: logger.warning(f"Ошибка тикера {имя_биржи}/{символ}: {e}"); continue
        if len(валидные_биржи_для_символа) < 2: continue

        # Находим лучший положительный спред и арбитражные возможности
        current_best_spread = Decimal("-inf")
        current_best_pair = "N/A"

        for i in range(len(валидные_биржи_для_символа)):
            for j in range(i + 1, len(валидные_биржи_для_символа)):
                ex1_name = валидные_биржи_для_символа[i]; ex2_name = валидные_биржи_для_символа[j]
                ticker1 = данные_по_символу[ex1_name]; ticker2 = данные_по_символу[ex2_name]
                ask1, bid1 = ticker1["ask"], ticker1["bid"]; ask2, bid2 = ticker2["ask"], ticker2["bid"]

                # Считаем оба направления
                profit_1_2 = ((bid2 - ask1) / ask1) * 100 if ask1 > 0 else Decimal("-inf")
                profit_2_1 = ((bid1 - ask2) / ask2) * 100 if ask2 > 0 else Decimal("-inf")

                # Обновляем лучший положительный спред для символа
                if profit_1_2 > current_best_spread and profit_1_2 >= 0:
                    current_best_spread = profit_1_2
                    current_best_pair = f"{ex1_name} -> {ex2_name}"
                if profit_2_1 > current_best_spread and profit_2_1 >= 0:
                    current_best_spread = profit_2_1
                    current_best_pair = f"{ex2_name} -> {ex1_name}"

                # Проверяем на арбитраж >= порога
                if profit_1_2 >= порог_decimal:
                    возможности.append({"символ": символ, "купить_биржа": ex1_name, "купить_цена": ask1, "продать_биржа": ex2_name, "продать_цена": bid2, "спред_проц": profit_1_2})
                if profit_2_1 >= порог_decimal:
                    возможности.append({"символ": символ, "купить_биржа": ex2_name, "купить_цена": ask2, "продать_биржа": ex1_name, "продать_цена": bid1, "спред_проц": profit_2_1})

        # Сохраняем лучший найденный положительный спред для символа
        if current_best_spread >= 0:
             best_positive_spreads[символ]["пара"] = current_best_pair
             best_positive_spreads[символ]["спред_проц"] = current_best_spread
        else: # Если не нашли положительных, оставляем N/A и -inf
             best_positive_spreads[символ]["пара"] = "N/A"
             best_positive_spreads[символ]["спред_проц"] = None # Или Decimal('-inf') или 0, чтобы не было ошибки

    return best_positive_spreads, возможности # Возвращаем словарь лучших спредов и список возможностей

def style_time_delta(delta_sec: float | None) -> str:
    """Возвращает CSS стиль для ячейки времени обновления."""
    if delta_sec is None: return "background-color: white;"
    if delta_sec <= TS_FRESH_THRESHOLD: return "background-color: #90EE90;" # lightgreen
    if delta_sec <= TS_STALE_THRESHOLD: return "background-color: #FFFFE0;" # lightyellow
    return "background-color: #F08080; color: white;" # lightcoral

def highlight_best_prices(row):
    """Возвращает стили для подсветки лучших цен."""
    # ---> ИЗМЕНЕНИЕ: Выравнивание по правому краю по умолчанию <---
    styles = ['text-align: right;'] * len(row.index)
    символ = row.name
    _, best_bid_ex = лучшие_биды.get(символ, (None, "")); _, best_ask_ex = лучшие_аски.get(символ, (None, ""))
    for i, col_name in enumerate(row.index):
        if best_bid_ex and f"{best_bid_ex} Предложение (Bid)" == col_name: styles[i] += " background-color: #90EE90; font-weight: bold;"
        if best_ask_ex and f"{best_ask_ex} Запрос (Ask)" == col_name: styles[i] += " background-color: #F08080; color: white; font-weight: bold;"
    return styles

# --- Основная часть Дашборда ---
актуальные_данные = загрузить_данные_из_бд()
if актуальные_данные is None:
    актуальные_данные = st.session_state.get("last_valid_db_data", None)
    if актуальные_данные: st.warning("Не удалось загрузить свежие данные. Отображаются последние доступные.")
    else: st.error("Не удалось загрузить данные."); st.stop()
else: st.session_state["last_valid_db_data"] = актуальные_данные

# --- Фильтры и Сохранение состояния ---
st.sidebar.header("Фильтры Отображения")
доступные_биржи = sorted(list(актуальные_данные.keys())); доступные_символы = sorted(config.SYMBOLS_TO_TRACK)

# Инициализация session_state
if 'выбранные_символы' not in st.session_state: st.session_state['выбранные_символы'] = доступные_символы
if 'выбранные_биржи' not in st.session_state: st.session_state['выбранные_биржи'] = доступные_биржи

def update_session_state_symbols(): st.session_state.выбранные_символы = st.session_state.symbol_filter_key
def update_session_state_exchanges(): st.session_state.выбранные_биржи = st.session_state.exchange_filter_key

выбранные_символы_widget = st.sidebar.multiselect( # Используем другое имя переменной для виджета
    "Символы:", options=доступные_символы, default=st.session_state.выбранные_символы,
    key="symbol_filter_key", on_change=update_session_state_symbols
)
выбранные_биржи_widget = st.sidebar.multiselect( # Используем другое имя переменной для виджета
    "Биржи:", options=доступные_биржи, default=st.session_state.выбранные_биржи,
    key="exchange_filter_key", on_change=update_session_state_exchanges
)

# Используем значения из session_state для логики
выбранные_символы = st.session_state.выбранные_символы
выбранные_биржи = st.session_state.выбранные_биржи

if not выбранные_символы: выбранные_символы = доступные_символы
if not выбранные_биржи: выбранные_биржи = доступные_биржи
show_spreads = len(выбранные_биржи) >= 2
if not show_spreads: st.sidebar.warning("Выберите минимум 2 биржи.")

# --- Расчеты ---
рассчитанные_спреды, арбитражные_ситуации = рассчитать_спреды(актуальные_данные, выбранные_символы, выбранные_биржи, config.ARBITRAGE_THRESHOLD_PCT)

# --- Метрики ---
col1, col2, col3 = st.columns(3)
active_ex_count = sum(1 for ex in выбранные_биржи if any(актуальные_данные.get(ex,{}).get(sym) for sym in выбранные_символы))
col1.metric("Активные биржи", f"{active_ex_count} / {len(выбранные_биржи)}")
col2.metric("Отслеживаемые пары", len(выбранные_символы))
col3.metric("Арбитражные ситуации", len(арбитражные_ситуации))
st.divider()

# --- Топ-3 Спреда ---
# --- Вкладки ---
tab_prices, tab_spreads, tab_arbitrage = st.tabs(["📊 Обзор Цен и Объемов", "📈 Анализ Спредов", "💰 Арбитраж"])

# === ВКЛАДКА: ОБЗОР ЦЕН И ОБЪЕМОВ ===
with tab_prices:
    st.subheader("Текущие цены (Bid/Ask)")
    список_цен_для_таблицы = []
    price_columns = ["Символ"] + [f"{ex} {type_}" for ex in выбранные_биржи for type_ in ["Предложение (Bid)", "Запрос (Ask)"]]
    лучшие_биды = {s: (Decimal("-inf"), "") for s in выбранные_символы}
    лучшие_аски = {s: (Decimal("inf"), "") for s in выбранные_символы}
    for символ in выбранные_символы:
        данные_строки = {"Символ": символ}
        for имя_биржи in выбранные_биржи:
            инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ); bid_col, ask_col = f"{имя_биржи} Предложение (Bid)", f"{имя_биржи} Запрос (Ask)"; bid_val, ask_val = None, None
            if isinstance(инфо_тикера, TickerData): bid_val, ask_val = инфо_тикера.bid_price, инфо_тикера.ask_price; данные_строки[bid_col] = f"{bid_val:.{DECIMAL_PLACES_PRICE}f}" if bid_val else "N/A"; данные_строки[ask_col] = f"{ask_val:.{DECIMAL_PLACES_PRICE}f}" if ask_val else "N/A";
            else: данные_строки[bid_col], данные_строки[ask_col] = "N/A", "N/A"
            if bid_val is not None and bid_val > лучшие_биды[символ][0]: лучшие_биды[символ] = (bid_val, имя_биржи)
            if ask_val is not None and ask_val < лучшие_аски[символ][0]: лучшие_аски[символ] = (ask_val, имя_биржи)
        список_цен_для_таблицы.append(данные_строки)
    if список_цен_для_таблицы:
        df_цены = pd.DataFrame(список_цен_для_таблицы, columns=price_columns).set_index("Символ")
        st.dataframe(df_цены.style.apply(highlight_best_prices, axis=1), use_container_width=True)
    else: st.info("Нет данных цен.")

    st.subheader("Суммы ордеров (USD) и время обновления")
    список_сумм_для_таблицы = []
    стили_сумм_для_таблицы = []
    volume_columns = ["Символ"] + [f"{ex} {type_}" for ex in выбранные_биржи for type_ in ["Сумма Bid (USD)", "Сумма Ask (USD)", "Обновлено (сек)"]]
    текущее_время_utc = datetime.now(timezone.utc)
    decimal_quantizer_usd = Decimal('1e-' + str(DECIMAL_PLACES_VOLUME_USD))
    for символ in выбранные_символы:
        данные_строки, данные_стиля_строки = {"Символ": символ}, {"Символ": ""}
        for имя_биржи in выбранные_биржи:
            инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ); bsum_col = f"{имя_биржи} Сумма Bid (USD)"; asum_col = f"{имя_биржи} Сумма Ask (USD)"; ts_col = f"{имя_биржи} Обновлено (сек)"; delta_s = None
            for col in [bsum_col, asum_col, ts_col]: данные_стиля_строки[col] = "" # Стили по умолчанию
            if isinstance(инфо_тикера, TickerData):
                b_s, a_s = инфо_тикера.bid_size, инфо_тикера.ask_size; b_p, a_p = инфо_тикера.bid_price, инфо_тикера.ask_price; ts_ms = инфо_тикера.timestamp_ms
                данные_строки[bsum_col] = str((b_s * b_p).quantize(decimal_quantizer_usd)) if b_s and b_p else "N/A"
                данные_строки[asum_col] = str((a_s * a_p).quantize(decimal_quantizer_usd)) if a_s and a_p else "N/A"
                if ts_ms and ts_ms > 0:
                    try: delta_s = (текущее_время_utc - datetime.fromtimestamp(ts_ms / 1000, timezone.utc)).total_seconds(); данные_строки[ts_col] = f"{delta_s:.1f}"
                    except: данные_строки[ts_col] = "Ошибка"
                else: данные_строки[ts_col] = "N/A"
                данные_стиля_строки[ts_col] = style_time_delta(delta_s) # Запоминаем стиль времени
            else: данные_строки[bsum_col] = "N/A"; данные_строки[asum_col] = "N/A"; данные_строки[ts_col] = "N/A"
        список_сумм_для_таблицы.append(данные_строки); стили_сумм_для_таблицы.append(данные_стиля_строки)
    if список_сумм_для_таблицы:
        df_суммы = pd.DataFrame(список_сумм_для_таблицы, columns=volume_columns).set_index("Символ")
        style_columns = [col for col in volume_columns if col != "Символ"]
        df_стили = pd.DataFrame(стили_сумм_для_таблицы, index=df_суммы.index, columns=style_columns)
        st.dataframe(df_суммы.style.apply(lambda r: df_стили.loc[r.name], axis=1)
                             .set_properties(**{'text-align': 'right'}, subset=[col for col in style_columns if 'Сумма' in col])
                             .set_properties(**{'text-align': 'center'}, subset=[col for col in style_columns if 'Обновлено' in col]),
                       use_container_width=True)
    else: st.info("Нет данных объемов.")

# === ВКЛАДКА: АНАЛИЗ СПРЕДОВ ===
with tab_spreads:
    st.subheader("Лучший положительный спред по парам (%)")

    лучшие_спреды = рассчитанные_спреды # Используем рассчитанные данные

    display_data_best_spreads = []
    # Сортируем символы для консистентного порядка
    for символ in sorted(выбранные_символы):
        best_info = лучшие_спреды.get(символ)
        best_spread_value = best_info.get("спред_проц") if best_info else None

        # Добавляем строку, только если есть положительный спред
        if best_spread_value is not None and best_spread_value >= 0 and best_spread_value != Decimal("-inf"):
            # Форматируем спред с цветом через Markdown
            spread_str = f"{best_spread_value:.{DECIMAL_PLACES_SPREAD}f}%"
            color = "black" # Цвет текста по умолчанию
            if best_spread_value >= config.ARBITRAGE_THRESHOLD_PCT: color = "green"
            elif best_spread_value > 0: color = "orange" # Используем оранжевый вместо желтого для лучшей видимости
            # elif best_spread_value == 0: color = "gray" # Можно и ноль выделить

            formatted_spread = f"<span style='color:{color}; font-weight:bold;'>{spread_str}</span>"

            display_data_best_spreads.append({
                 "Символ": символ,
                 "Лучшая пара": best_info["пара"],
                 "Спред (%)": formatted_spread # Сохраняем HTML строку
             })
        # Можно добавить else, чтобы показывать все строки, но с прочерком
        # else:
        #     display_data_best_spreads.append({
        #         "Символ": символ, "Лучшая пара": "-", "Спред (%)": "-"
        #     })

    if display_data_best_spreads:
        # Создаем DataFrame
        df_best_spreads = pd.DataFrame(display_data_best_spreads)
        # Конвертируем DataFrame в HTML и разрешаем рендеринг HTML
        st.markdown(df_best_spreads.to_html(escape=False, index=False), unsafe_allow_html=True)
        # Отображение через st.table - не будет рендерить HTML
        # st.table(df_best_spreads.set_index("Символ"))

    else:
        st.info("Положительных спредов для выбранных фильтров не найдено.")

# === ВКЛАДКА: АРБИТРАЖ ===
with tab_arbitrage:
    st.subheader(
        f"Найденные арбитражные ситуации (спред >= {config.ARBITRAGE_THRESHOLD_PCT}%)"
    )
    отфильтрованные_ситуации = [
        op for op in арбитражные_ситуации
        if op["символ"] in выбранные_символы
        and op["купить_биржа"] in выбранные_биржи
        and op["продать_биржа"] in выбранные_биржи
    ]
    if отфильтрованные_ситуации:
        отфильтрованные_ситуации.sort(key=lambda x: x["спред_проц"], reverse=True)
        for ситуация in отфильтрованные_ситуации: # <-- Используем переменную 'ситуация'
            # ---> ИСПРАВЛЕНИЕ: Используем 'ситуация' для доступа к данным <---
            цена_покупки_стр = (
                f"{ситуация['купить_цена']:.{DECIMAL_PLACES_PRICE}f}"
            )
            цена_продажи_стр = (
                f"{ситуация['продать_цена']:.{DECIMAL_PLACES_PRICE}f}"
            )
            спред_стр = ( # <-- Определяем переменную 'спред_стр'
                f"{ситуация['спред_проц']:.{DECIMAL_PLACES_SPREAD}f}%"
            )
            # ---> КОНЕЦ ИСПРАВЛЕНИЯ <---
            st.success(
                f"**{ситуация['символ']}**: Купить **{ситуация['купить_биржа']}** @ {цена_покупки_стр} "
                f"➡️ Продать **{ситуация['продать_биржа']}** @ {цена_продажи_стр}. **Спред: {спред_стр}**"
            )
    else:
        st.info(
            f"Арбитражных ситуаций >= {config.ARBITRAGE_THRESHOLD_PCT}% не найдено."
        )
# --- Время обновления ---
st.caption(f"Дашборд обновлен: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")
st_autorefresh(interval=REFRESH_INTERVAL_SECONDS * 1000, key="обновлятор_данных")