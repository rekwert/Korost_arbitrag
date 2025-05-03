import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import numpy as np
import pandas as pd
import streamlit as st
import time

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
DECIMAL_PLACES_PRICE = 5
DECIMAL_PLACES_SIZE = 4
DECIMAL_PLACES_SPREAD = 4
DECIMAL_PLACES_VOLUME_USD = 2
TS_FRESH_THRESHOLD = 5.0
TS_STALE_THRESHOLD = 15.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - DASHBOARD - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Настройки Streamlit ---
st.set_page_config(
    page_title="Арбитражный Монитор", page_icon="🚀", layout="wide"
)
st.title("🚀 Арбитражный Монитор Криптовалютных Бирж (Redis)")


# --- Функции ---
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

def рассчитать_спреды(
    данные: dict, символы: list[str], биржи: list[str], порог: Decimal
):
    """Рассчитывает спреды и находит арбитражные возможности."""
    все_спреды = {}
    возможности = []
    try: порог_decimal = Decimal(str(порог))
    except: порог_decimal = Decimal('0.1')
    for символ in символы:
        все_спреды[символ] = {}; данные_по_символу = {}; валидные_биржи = []
        for имя_биржи in биржи:
            if имя_биржи not in данные: continue
            инфо_тикера = данные.get(имя_биржи, {}).get(символ)
            try:
                if (isinstance(инфо_тикера, TickerData) and
                        инфо_тикера.bid_price and инфо_тикера.ask_price and
                        инфо_тикера.bid_price > 0 and инфо_тикера.ask_price > 0):
                    данные_по_символу[имя_биржи] = {"bid": инфо_тикера.bid_price, "ask": инфо_тикера.ask_price, "ts": инфо_тикера.timestamp_ms}
                    валидные_биржи.append(имя_биржи)
            except Exception as e: logger.warning(f"Ошибка тикера {имя_биржи}/{символ}: {e}")
        if len(валидные_биржи) < 2: continue
        for i in range(len(валидные_биржи)):
            for j in range(i + 1, len(валидные_биржи)):
                ex1, ex2 = валидные_биржи[i], валидные_биржи[j]
                t1, t2 = данные_по_символу[ex1], данные_по_символу[ex2]
                ask1, bid1 = t1["ask"], t1["bid"]; ask2, bid2 = t2["ask"], t2["bid"]
                p12 = ((bid2 - ask1) / ask1) * 100 if ask1 > 0 else Decimal('-inf'); p21 = ((bid1 - ask2) / ask2) * 100 if ask2 > 0 else Decimal('-inf')
                k12, k21 = f"{ex1} -> {ex2}", f"{ex2} -> {ex1}"
                все_спреды[символ][k12], все_спреды[символ][k21] = p12, p21
                if p12 >= порог_decimal: возможности.append({"символ": символ, "купить_биржа": ex1, "купить_цена": ask1, "продать_биржа": ex2, "продать_цена": bid2, "спред_проц": p12})
                if p21 >= порог_decimal: возможности.append({"символ": символ, "купить_биржа": ex2, "купить_цена": ask2, "продать_биржа": ex1, "продать_цена": bid1, "спред_проц": p21})
    return все_спреды, возможности

def style_time_delta(delta_sec: float | None) -> str:
    """Возвращает CSS стиль для ячейки времени обновления."""
    if delta_sec is None: return "background-color: white;"
    if delta_sec <= TS_FRESH_THRESHOLD: return "background-color: #90EE90;"
    if delta_sec <= TS_STALE_THRESHOLD: return "background-color: #FFFFE0;"
    return "background-color: #F08080; color: white;"

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
if 'выбранные_символы' not in st.session_state: st.session_state['выбранные_символы'] = доступные_символы
if 'выбранные_биржи' not in st.session_state: st.session_state['выбранные_биржи'] = доступные_биржи
if 'порог_арбитража' not in st.session_state: st.session_state['порог_арбитража'] = float(config.ARBITRAGE_THRESHOLD_PCT)
if 'выбранный_символ_детали' not in st.session_state: st.session_state['выбранный_символ_детали'] = None

def update_session_state(key): st.session_state[key.replace('_filter_key','')] = st.session_state[key]
def update_threshold(): st.session_state.порог_арбитража = st.session_state.threshold_key

выбранные_символы_widget = st.sidebar.multiselect("Символы:", options=доступные_символы, default=st.session_state.выбранные_символы, key="symbol_filter_key", on_change=update_session_state, args=("symbol_filter_key",))
выбранные_биржи_widget = st.sidebar.multiselect("Биржи:", options=доступные_биржи, default=st.session_state.выбранные_биржи, key="exchange_filter_key", on_change=update_session_state, args=("exchange_filter_key",))
порог_арб_ввод = st.sidebar.number_input("Порог арбитража (%):", min_value=0.0, max_value=5.0, step=0.01, value=st.session_state.порог_арбитража, key="threshold_key", on_change=update_threshold, format="%.4f")

выбранные_символы = st.session_state.выбранные_символы; выбранные_биржи = st.session_state.выбранные_биржи; порог_арбитража_для_расчета = Decimal(str(st.session_state.порог_арбитража))
if not выбранные_символы: выбранные_символы = доступные_символы
if not выбранные_биржи: выбранные_биржи = доступные_биржи
show_spreads = len(выбранные_биржи) >= 2
if not show_spreads: st.sidebar.warning("Выберите минимум 2 биржи.")

# --- Расчеты ---
рассчитанные_спреды, арбитражные_ситуации = рассчитать_спреды(актуальные_данные, выбранные_символы, выбранные_биржи, порог_арбитража_для_расчета)

# --- Метрики ---
col1, col2, col3 = st.columns(3); active_ex_count = sum(1 for ex in выбранные_биржи if any(актуальные_данные.get(ex,{}).get(sym) for sym in выбранные_символы)); col1.metric("Активные биржи", f"{active_ex_count} / {len(выбранные_биржи)}"); col2.metric("Отслеживаемые пары", len(выбранные_символы)); filtered_ops_metric = [op for op in арбитражные_ситуации if op["символ"] in выбранные_символы and op["купить_биржа"] in выбранные_биржи and op["продать_биржа"] in выбранные_биржи]; col3.metric("Арбитражные ситуации", len(filtered_ops_metric)); st.divider()

# --- Топ-3 Спреда ---
st.subheader("Топ-3 Текущих Положительных Спреда"); top_spreads = [];
for символ, спреды_пары in рассчитанные_спреды.items():
    if символ not in выбранные_символы: continue
    for пара, спред in спреды_пары.items():
        try: ex1, ex2 = пара.split(" -> ")
        except ValueError: continue
        if ex1 not in выбранные_биржи or ex2 not in выбранные_биржи: continue
        if isinstance(спред, Decimal) and спред > 0: top_spreads.append({"Символ": символ, "Пара": пара, "Спред (%)": спред})
if top_spreads: top_spreads.sort(key=lambda x: x["Спред (%)"], reverse=True); cols_top = st.columns(min(3, len(top_spreads)));
else: st.info("Положительных спредов не найдено.")
st.divider()

# --- Вкладки ---
tab_prices, tab_spreads, tab_arbitrage = st.tabs(["📊 Обзор и Детали", "📈 Лучший Спред", "💰 Арбитраж"])

# === ВКЛАДКА: ОБЗОР ЦЕН И ОБЪЕМОВ ===
with tab_prices:
    st.subheader("Обзор лучших цен по парам")
    текущее_время_utc = datetime.now(timezone.utc)
    обзор_данных_list = []
    обзор_columns = ["Символ", "Лучший Bid (Биржа)", "Лучший Ask (Биржа)", "Обновлено (сек)"]

    лучшие_биды = {s: (Decimal("-inf"), "") for s in выбранные_символы}
    лучшие_аски = {s: (Decimal("inf"), "") for s in выбранные_символы}
    for символ in выбранные_символы:
        latest_ts_symbol = 0
        for имя_биржи in выбранные_биржи:
            инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ)
            if isinstance(инфо_тикера, TickerData):
                bid_val, ask_val = инфо_тикера.bid_price, инфо_тикера.ask_price
                if bid_val is not None and bid_val > лучшие_биды[символ][0]: лучшие_биды[символ] = (bid_val, имя_биржи)
                if ask_val is not None and ask_val < лучшие_аски[символ][0]: лучшие_аски[символ] = (ask_val, имя_биржи)
                if инфо_тикера.timestamp_ms and инфо_тикера.timestamp_ms > latest_ts_symbol: latest_ts_symbol = инфо_тикера.timestamp_ms
        best_bid_val, best_bid_ex = лучшие_биды[символ]; best_ask_val, best_ask_ex = лучшие_аски[символ]
        bid_str = f"{best_bid_val:.{DECIMAL_PLACES_PRICE}f} ({best_bid_ex})" if best_bid_val > Decimal("-inf") else "N/A"
        ask_str = f"{best_ask_val:.{DECIMAL_PLACES_PRICE}f} ({best_ask_ex})" if best_ask_val < Decimal("inf") else "N/A"
        delta_s = None; ts_str = "N/A"
        if latest_ts_symbol > 0:
            try: delta_s = (текущее_время_utc - datetime.fromtimestamp(latest_ts_symbol / 1000, timezone.utc)).total_seconds(); ts_str = f"{delta_s:.1f}";
            except Exception as e_ts: logger.warning(f"Ошибка времени {символ}: {e_ts}"); ts_str = "Ошибка"
        обзор_данных_list.append({"Символ": символ, "Лучший Bid (Биржа)": bid_str, "Лучший Ask (Биржа)": ask_str, "Обновлено (сек)": ts_str, "_delta_s": delta_s})

    if обзор_данных_list:
        df_обзор = pd.DataFrame(обзор_данных_list)
        # ---> ИСПРАВЛЕНИЕ: Убираем apply для таблицы обзора <---
        st.dataframe(
            df_обзор.drop(columns=['_delta_s']).set_index('Символ'), # Устанавливаем индекс после drop
            use_container_width=True
            # Стилизация через set_properties может вызвать ту же ошибку, убираем пока
            # .style.set_properties(**{'text-align': 'left'}, subset=['Символ']) # Не сработает с set_index
            # .set_properties(**{'text-align': 'right'}, subset=['Лучший Bid (Биржа)', 'Лучший Ask (Биржа)'])
            # .set_properties(**{'text-align': 'center'}, subset=['Обновлено (сек)'])
        )
    else: st.info("Нет данных для обзора.")
    st.divider()

    st.subheader("Детали по выбранной паре")
    выбранный_символ_детали = st.selectbox("Выберите символ:", options=[""] + выбранные_символы, key="symbol_details_selector", index=0, label_visibility="collapsed")
    if выбранный_символ_детали:
        with st.container():
            st.caption(f"Детали по **{выбранный_символ_детали}**:")
            details_cols = st.columns(len(выбранные_биржи))
            for idx, ex_name in enumerate(выбранные_биржи):
                ticker_info = актуальные_данные.get(ex_name, {}).get(выбранный_символ_детали)
                with details_cols[idx]:
                    st.markdown(f"**{ex_name}**")
                    if isinstance(ticker_info, TickerData):
                        b_p, a_p = ticker_info.bid_price, ticker_info.ask_price; b_s, a_s = ticker_info.bid_size, ticker_info.ask_size; ts_ms = ticker_info.timestamp_ms; delta_s_detail = None
                        if ts_ms and ts_ms > 0:
                            try: delta_s_detail = (текущее_время_utc - datetime.fromtimestamp(ts_ms / 1000, timezone.utc)).total_seconds();
                            except: pass
                        st.markdown(f"Bid: {b_p:.{DECIMAL_PLACES_PRICE}f}" if b_p else "-"); st.markdown(f"Ask: {a_p:.{DECIMAL_PLACES_PRICE}f}" if a_p else "-")
                        st.markdown(f"Vol Bid: {b_s:.{DECIMAL_PLACES_SIZE}f}" if b_s else "-"); st.markdown(f"Vol Ask: {a_s:.{DECIMAL_PLACES_SIZE}f}" if a_s else "-")
                        sum_bid_usd = (b_s * b_p).quantize(Decimal('1e-2')) if b_s and b_p else None; sum_ask_usd = (a_s * a_p).quantize(Decimal('1e-2')) if a_s and a_p else None
                        st.markdown(f"Sum Bid: ${sum_bid_usd:,.2f}" if sum_bid_usd is not None else "-"); st.markdown(f"Sum Ask: ${sum_ask_usd:,.2f}" if sum_ask_usd is not None else "-")
                        st.markdown(f"<div style='{style_time_delta(delta_s_detail)} padding: 1px 4px; border-radius: 3px; display: inline-block; width: fit-content;'>Upd: {delta_s_detail:.1f}s</div>" if delta_s_detail is not None else "Upd: -", unsafe_allow_html=True) # Добавил display:inline-block
                    else: st.text("Нет данных")

# === ВКЛАДКА: АНАЛИЗ СПРЕДОВ ===
with tab_spreads:
    # ---> Используем вариант с HTML таблицей для лучших спредов <---
    st.subheader("Лучший положительный спред по парам (%)")
    if show_spreads:
        лучшие_спреды_для_таблицы = []; min_spread_val_all = (Decimal("inf"), "", ""); max_spread_val_pos = (Decimal("-inf"), "", "")
        for символ in выбранные_символы:
            best_info_for_symbol = {"пара": "-", "спред_проц": None}; current_best_spread = Decimal("-inf")
            if символ in рассчитанные_спреды and рассчитанные_спреды[символ]:
                for ключ_пары, спред_проц in рассчитанные_спреды[символ].items():
                    try: ex1, ex2 = ключ_пары.split(" -> ")
                    except ValueError: continue
                    if ex1 not in выбранные_биржи or ex2 not in выбранные_биржи: continue
                    if спред_проц is not None and спред_проц != Decimal("-inf"):
                        if спред_проц < min_spread_val_all[0]: min_spread_val_all = (спред_проц, символ, ключ_пары)
                        if спред_проц >= 0 and спред_проц > current_best_spread: current_best_spread = спред_проц; best_info_for_symbol["пара"] = ключ_пары; best_info_for_symbol["спред_проц"] = спред_проц
                        if спред_проц > max_spread_val_pos[0]: max_spread_val_pos = (спред_проц, символ, ключ_пары)
            formatted_spread = "-"
            best_spread_value = best_info_for_symbol.get("спред_проц")
            if best_spread_value is not None and best_spread_value >= 0:
                spread_str = f"{best_spread_value:.{DECIMAL_PLACES_SPREAD}f}%"; color = "orange"; font_weight = "bold";
                if best_spread_value >= порог_арбитража_для_расчета: color = "green"
                formatted_spread = f"<p style='color:{color}; font-weight:{font_weight}; margin:0; text-align:right;'>{spread_str}</p>"
            лучшие_спреды_для_таблицы.append({"Символ": символ, "Лучшая пара": best_info_for_symbol["пара"], "Спред (%)": formatted_spread})
        if лучшие_спреды_для_таблицы:
            df_best_spreads = pd.DataFrame(лучшие_спреды_для_таблицы)
            st.markdown(df_best_spreads.to_html(escape=False, index=False, justify="center", border=0, classes="best-spread-table"), unsafe_allow_html=True)
            st.markdown("""<style>
            .best-spread-table { width: 90%; margin-left: auto; margin-right: auto; border-collapse: collapse; }
            .best-spread-table th, .best-spread-table td { padding: 5px 10px; text-align: center; vertical-align: middle;}
            .best-spread-table th { border-bottom: 1px solid #ddd; text-align: center; font-weight: bold;}
            .best-spread-table td:nth-child(1) { text-align: left; font-weight: bold; width: 15%;}
            .best-spread-table td:nth-child(2) { text-align: center; width: 45%;}
            .best-spread-table td:nth-child(3) p { text-align: right !important; padding: 0; width: 40%;}
            </style>""", unsafe_allow_html=True)
            if max_spread_val_pos[0] > Decimal("-inf"): st.caption(f"📈 Макс. +спред: {max_spread_val_pos[1]} ({max_spread_val_pos[2]}) = {max_spread_val_pos[0]:.{DECIMAL_PLACES_SPREAD}f}%")
            if min_spread_val_all[0] < Decimal("inf"): st.caption(f"📉 Мин. -спред: {min_spread_val_all[1]} ({min_spread_val_all[2]}) = {min_spread_val_all[0]:.{DECIMAL_PLACES_SPREAD}f}%")
        else: st.info("Положительных спредов не найдено.")
    else: st.warning("Выберите минимум 2 биржи.")

# === ВКЛАДКА: АРБИТРАЖ ===
with tab_arbitrage:
    st.subheader(f"Найденные арбитражные ситуации (спред >= {st.session_state.порог_арбитража}%)")
    filtered_ops = [op for op in арбитражные_ситуации if op["символ"] in выбранные_символы and op["купить_биржа"] in выбранные_биржи and op["продать_биржа"] in выбранные_биржи]
    if filtered_ops:
        filtered_ops.sort(key=lambda x: x["спред_проц"], reverse=True)
        for ситуация in filtered_ops:
            цена_покупки_стр = f"{ситуация['купить_цена']:.{DECIMAL_PLACES_PRICE}f}"; цена_продажи_стр = f"{ситуация['продать_цена']:.{DECIMAL_PLACES_PRICE}f}"; спред_стр = f"{ситуация['спред_проц']:.{DECIMAL_PLACES_SPREAD}f}%"
            st.success(f"**{ситуация['символ']}**: Купить **{ситуация['купить_биржа']}** @ {цена_покупки_стр} ➡️ Продать **{ситуация['продать_биржа']}** @ {цена_продажи_стр}. **Спред: {спред_стр}**")
    else: st.info(f"Арбитражных ситуаций >= {st.session_state.порог_арбитража}% не найдено.")

# --- Время обновления ---
st.caption(f"Дашборд обновлен: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")

# --- Автообновление ---
st_autorefresh(interval=REFRESH_INTERVAL_SECONDS * 1000, key="обновлятор_данных")
