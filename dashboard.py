# dashboard.py
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import numpy as np  # Добавляем numpy
import pandas as pd
import streamlit as st

try:
    import config
    import data_store
    from models import TickerData
except ModuleNotFoundError:
    st.error(
        "Ошибка: Не найдены модули data_store.py, config.py или models.py."
    )
    st.stop()

from streamlit_autorefresh import st_autorefresh

# --- Настройки ---
REFRESH_INTERVAL_SECONDS = 3
DECIMAL_PLACES_PRICE = 8
DECIMAL_PLACES_SPREAD = 4

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - DASHBOARD - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Настройки Streamlit ---
st.set_page_config(
    page_title="Арбитражный Монитор", page_icon="🚀", layout="wide"
)

st.title("🚀 Арбитражный Монитор Криптовалютных Бирж (Redis)")


# --- Получение данных ---
def загрузить_данные_из_бд() -> dict | None:
    logger.info("Загрузка данных из Redis...")
    try:
        exchanges_to_load = [
            config.BYBIT_EXCHANGE_NAME,
            config.BINANCE_EXCHANGE_NAME,
            config.MEXC_EXCHANGE_NAME,
            config.KUCOIN_EXCHANGE_NAME,
            # config.HTX_EXCHANGE_NAME, # HTX отключен
        ]
        data = data_store.get_all_tickers_sync(
            exchanges_to_load, config.SYMBOLS_TO_TRACK
        )
        logger.info(f"Данные из Redis загружены. Ключей бирж: {len(data)}")
        return data
    except Exception as e:
        logger.error(
            f"Ошибка при вызове get_all_tickers_sync: {e}", exc_info=True
        )
        st.error(f"Ошибка загрузки данных из Redis: {e}")
        return None


актуальные_данные = загрузить_данные_из_бд()

if актуальные_данные is None:
    актуальные_данные = st.session_state.get("last_valid_db_data", None)
    if актуальные_данные:
        st.warning(
            "Не удалось загрузить свежие данные из Redis. Отображаются последние доступные."
        )
    else:
        st.error(
            "Не удалось загрузить данные из Redis. Проверьте работу `main.py` и доступность Redis."
        )
        st.stop()
else:
    st.session_state["last_valid_db_data"] = актуальные_данные

# --- Фильтры ---
st.sidebar.header("Фильтры Отображения")
доступные_биржи = sorted(list(актуальные_данные.keys()))
доступные_символы = sorted(config.SYMBOLS_TO_TRACK)

выбранные_символы = st.sidebar.multiselect(
    "Символы:", options=доступные_символы, default=доступные_символы
)
выбранные_биржи = st.sidebar.multiselect(
    "Биржи:", options=доступные_биржи, default=доступные_биржи
)

if not выбранные_символы:
    выбранные_символы = доступные_символы
if not выбранные_биржи or len(выбранные_биржи) < 2:
    выбранные_биржи = доступные_биржи
    if len(выбранные_биржи) < 2:
        st.sidebar.warning("Для расчета спредов нужно выбрать минимум 2 биржи.")


# --- Расчеты ---
def рассчитать_спреды(
    данные: dict, символы: list[str], биржи: list[str], порог: Decimal
):
    все_спреды = {}
    возможности = []
    try:
        порог_decimal = Decimal(str(порог))
    except (InvalidOperation, TypeError):
        logger.error(
            f"Неверное значение порога арбитража: {порог}. Использую 0.1."
        )
        порог_decimal = Decimal("0.1")

    for символ in символы:
        все_спреды[символ] = {}
        данные_по_символу = {}
        валидные_биржи_для_символа = []

        for имя_биржи in биржи:
            if имя_биржи not in данные:
                continue
            инфо_тикера = данные.get(имя_биржи, {}).get(символ)
            try:
                if (
                    isinstance(инфо_тикера, TickerData)
                    and инфо_тикера.bid_price is not None # Проверяем не None
                    and инфо_тикера.ask_price is not None # Проверяем не None
                ):
                    if инфо_тикера.bid_price > 0 and инфо_тикера.ask_price > 0:
                        данные_по_символу[имя_биржи] = {
                            "bid": инфо_тикера.bid_price,
                            "ask": инфо_тикера.ask_price,
                            "ts": инфо_тикера.timestamp_ms,
                        }
                        валидные_биржи_для_символа.append(имя_биржи)
            except Exception as e:
                logger.warning(
                    f"Ошибка обработки тикера для {имя_биржи}/{символ} в рассчитать_спреды: {e}"
                )
                continue

        if len(валидные_биржи_для_символа) < 2:
            continue

        for i in range(len(валидные_биржи_для_символа)):
            for j in range(i + 1, len(валидные_биржи_для_символа)):
                биржа1_имя = валидные_биржи_для_символа[i]
                биржа2_имя = валидные_биржи_для_символа[j]
                тикер1 = данные_по_символу[биржа1_имя]
                тикер2 = данные_по_символу[биржа2_имя]

                ask1 = тикер1["ask"]
                bid1 = тикер1["bid"]
                ask2 = тикер2["ask"]
                bid2 = тикер2["bid"]

                профит_1_2_проц = (
                    ((bid2 - ask1) / ask1) * 100 if ask1 > 0 else Decimal("-inf")
                )
                профит_2_1_проц = (
                    ((bid1 - ask2) / ask2) * 100 if ask2 > 0 else Decimal("-inf")
                )

                ключ_пары_1_2 = f"{биржа1_имя} -> {биржа2_имя}"
                ключ_пары_2_1 = f"{биржа2_имя} -> {биржа1_имя}"
                # Сохраняем как Decimal для стилизации
                все_спреды[символ][ключ_пары_1_2] = профит_1_2_проц
                все_спреды[символ][ключ_пары_2_1] = профит_2_1_проц

                if профит_1_2_проц >= порог_decimal:
                    возможности.append(
                        {
                            "символ": символ,
                            "купить_биржа": биржа1_имя,
                            "купить_цена": ask1,
                            "продать_биржа": биржа2_имя,
                            "продать_цена": bid2,
                            "спред_проц": профит_1_2_проц,
                        }
                    )
                if профит_2_1_проц >= порог_decimal:
                    возможности.append(
                        {
                            "символ": символ,
                            "купить_биржа": биржа2_имя,
                            "купить_цена": ask2,
                            "продать_биржа": биржа1_имя,
                            "продать_цена": bid1,
                            "спред_проц": профит_2_1_проц,
                        }
                    )

    return все_спреды, возможности


рассчитанные_спреды, арбитражные_ситуации = рассчитать_спреды(
    актуальные_данные,
    выбранные_символы,
    выбранные_биржи,
    config.ARBITRAGE_THRESHOLD_PCT,
)

# --- Отображение основной таблицы цен ---
st.subheader("Текущие цены (Bid/Ask)")
список_цен_для_таблицы = []
текущее_время_utc = datetime.now(timezone.utc)

# Заранее определяем порядок колонок
columns_order = ["Символ"]
for имя_биржи in выбранные_биржи:
    columns_order.extend(
        [
            f"{имя_биржи} Предложение (Bid)",
            f"{имя_биржи} Запрос (Ask)",
            f"{имя_биржи} Обновлено (сек)",
        ]
    )

for символ in выбранные_символы:
    данные_строки = {"Символ": символ}
    for имя_биржи in выбранные_биржи:
        инфо_тикера = актуальные_данные.get(имя_биржи, {}).get(символ)
        bid_col = f"{имя_биржи} Предложение (Bid)"
        ask_col = f"{имя_биржи} Запрос (Ask)"
        ts_col = f"{имя_биржи} Обновлено (сек)"
        if isinstance(инфо_тикера, TickerData):
            данные_строки[bid_col] = (
                f"{инфо_тикера.bid_price:.{DECIMAL_PLACES_PRICE}f}"
                if инфо_тикера.bid_price is not None
                else "N/A"
            )
            данные_строки[ask_col] = (
                f"{инфо_тикера.ask_price:.{DECIMAL_PLACES_PRICE}f}"
                if инфо_тикера.ask_price is not None
                else "N/A"
            )
            if инфо_тикера.timestamp_ms and инфо_тикера.timestamp_ms > 0:
                try:
                    объект_времени = datetime.fromtimestamp(
                        инфо_тикера.timestamp_ms / 1000, timezone.utc
                    )
                    разница_времени = текущее_время_utc - объект_времени
                    данные_строки[ts_col] = f"{разница_времени.total_seconds():.1f}"
                except Exception:
                    данные_строки[ts_col] = "Ошибка"
            else:
                данные_строки[ts_col] = "N/A"
        else:
            данные_строки[bid_col] = "N/A"
            данные_строки[ask_col] = "N/A"
            данные_строки[ts_col] = "N/A"
    список_цен_для_таблицы.append(данные_строки)

if список_цен_для_таблицы:
    df_цены = pd.DataFrame(список_цен_для_таблицы, columns=columns_order).set_index(
        "Символ"
    )
    st.dataframe(df_цены, use_container_width=True)
else:
    st.info("Нет данных цен для отображения с учетом выбранных фильтров.")


# --- Отображение таблицы спредов с подсветкой ---
st.subheader("Рассчитанные спреды (%)")
список_спредов_для_таблицы = []
все_ключи_пар_выбранных = set()

if len(выбранные_биржи) >= 2:
    for i in range(len(выбранные_биржи)):
        for j in range(i + 1, len(выбранные_биржи)):
            ex1 = выбранные_биржи[i]
            ex2 = выбранные_биржи[j]
            все_ключи_пар_выбранных.add(f"{ex1} -> {ex2}")
            все_ключи_пар_выбранных.add(f"{ex2} -> {ex1}")
отсортированные_ключи_пар = sorted(list(все_ключи_пар_выбранных))

# Собираем данные для DataFrame, оставляя числа как Decimal
data_for_styling = []
for символ in выбранные_символы:
    if символ in рассчитанные_спреды and рассчитанные_спреды[символ]:
        данные_строки = {"Символ": символ}
        спреды_символа = рассчитанные_спреды[символ]
        for ключ_пары in отсортированные_ключи_пар:
            спред_проц = спреды_символа.get(ключ_пары)
            # Оставляем Decimal или None для стилизации
            данные_строки[ключ_пары] = (
                спред_проц if спред_проц != Decimal("-inf") else None
            )
        data_for_styling.append(данные_строки)

if data_for_styling:
    df_спреды_стиль = pd.DataFrame(data_for_styling).set_index("Символ")


    def color_spread_styler(val):
        """Красит фон: >= порога - зеленый, >= 0 - желтый, < 0 - красный."""
        color = 'white'  # Цвет по умолчанию для не-чисел
        text_color = 'black'
        try:
            # Проверяем, что это Decimal перед сравнением
            if isinstance(val, Decimal):
                # Сначала проверяем на порог арбитража
                if val >= config.ARBITRAGE_THRESHOLD_PCT:
                    color = 'lightgreen'
                    text_color = 'black'
                # Затем проверяем на НЕОТРИЦАТЕЛЬНОСТЬ (>= 0)
                elif val >= 0:  # <--- ИЗМЕНЕНИЕ ЗДЕСЬ (было > 0)
                    color = 'lightyellow'
                    text_color = 'black'
                # Иначе (если строго < 0)
                elif val < 0:
                    color = 'lightcoral'
                    text_color = 'white'
                # Неявный else: если не Decimal, остается 'white'/'black'
        except Exception as e:
            # На случай непредвиденных ошибок сравнения
            logger.error(f"Ошибка в color_spread_styler для значения {val}: {e}")
            pass  # Оставляем цвет по умолчанию

        return f'background-color: {color}; color: {text_color}'

    # Применяем стиль и форматирование
    st.dataframe(
        df_спреды_стиль.style.apply(
            lambda x: x.map(color_spread_styler), # Используем apply с map
            subset=отсортированные_ключи_пар
        ).format(
            "{:." + str(DECIMAL_PLACES_SPREAD) + "f}%", # Форматируем как процент
            subset=отсортированные_ключи_пар,
            na_rep="N/A", # Представление для None/NaN
        ),
        use_container_width=True,
    )

else:
    st.info("Нет данных спредов для отображения с учетом выбранных фильтров.")


# --- Отображение арбитражных ситуаций ---
st.subheader(
    f"Найденные арбитражные ситуации (спред >= {config.ARBITRAGE_THRESHOLD_PCT}%)"
)
отфильтрованные_ситуации = [
    опция
    for опция in арбитражные_ситуации
    if опция["символ"] in выбранные_символы
    and опция["купить_биржа"] in выбранные_биржи
    and опция["продать_биржа"] in выбранные_биржи
]

if отфильтрованные_ситуации:
    отфильтрованные_ситуации.sort(key=lambda x: x["спред_проц"], reverse=True)
    for ситуация in отфильтрованные_ситуации:
        цена_покупки_стр = f"{ситуация['купить_цена']:.{DECIMAL_PLACES_PRICE}f}"
        цена_продажи_стр = f"{ситуация['продать_цена']:.{DECIMAL_PLACES_PRICE}f}"
        спред_стр = f"{ситуация['спред_проц']:.{DECIMAL_PLACES_SPREAD}f}%"
        st.success(
            f"**{ситуация['символ']}**: Купить **{ситуация['купить_биржа']}** @ {цена_покупки_стр} "
            f"➡️ Продать **{ситуация['продать_биржа']}** @ {цена_продажи_стр}. "
            f"**Спред: {спред_стр}**"
        )
else:
    st.info(
        f"Арбитражных ситуаций с порогом >= {config.ARBITRAGE_THRESHOLD_PCT}% для выбранных фильтров не найдено."
    )

# --- Время обновления ---
st.caption(
    f"Дашборд обновлен: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}"
)

# --- Запуск автообновления ---
st_autorefresh(
    interval=REFRESH_INTERVAL_SECONDS * 1000, key="обновлятор_данных"
)