import asyncio
import logging
from typing import Tuple, Dict, Any
# Импортируем промпт и схему
from prompts import FILTER_INITIAL, FILTER_INITIAL_SCHEMA, ESSENCE_FILTRATION, ESSENCE_FILTRATION_SCHEMA
# Импортируем функцию обращения к API Deepseek
from .deepseek_service import call_deepseek_api

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def process_message_by_form(post_id: int, text_content: str) -> Tuple[bool, str, float, str]:

    """
    Основная функция для анализа сообщения: 
    формирует запрос к Deepseek API, получает структурированный ответ и возвращает результаты.

    Аргументы:
        post_id (int): ID записи в БД.
        text_content (str): Текст сообщения для анализа.

    Возвращает:
        Tuple[bool, str, bool, str]: 
        (filter_initial, filter_initial_explain, context, context_explain)
    """
    
    # Инициализируем значения по умолчанию
    filter_initial: bool = False
    filter_initial_explain: str = "Анализ не выполнен"

    try:

        # --- ВЫЗОВ СЛУЖБЫ DEEPSEEK для первичной фильтрации ---
        filter_initial_result_json = await call_deepseek_api(
            prompt=FILTER_INITIAL,
            text=text_content,
            response_schema=FILTER_INITIAL_SCHEMA,
            model_type='deepseek-chat',
            temperature=0.2
        )
        # -----------------------------

    except Exception as e:
        # Если API-вызов завершился критической ошибкой
        logging.error(f"MsgHandler: Критическая ошибка при формальном анализе поста ID:{post_id}: {e}")
        # Возвращаем None для filter_initial и сообщение об ошибке
        return False, f"Критическая ошибка API: {e}"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if filter_initial_result_json:

        filter_initial = filter_initial_result_json.get("filter", False)
        filter_initial_explain = filter_initial_result_json.get("explain", "Нет объяснения от ИИ.")
            
    else:
        logging.warning(f"MsgHandler: Deepseek вернул пустой ответ для поста ID:{post_id}.")
        filter_initial = False
        filter_initial_explain = "Невалидный или пустой ответ от ИИ"

    # Возвращаем требуемую пару
    return filter_initial, filter_initial_explain




async def process_message_by_essence(post_id: int, text_content: str) -> Tuple[float, float, str]:

    # Инициализируем значения по умолчанию
    emotional_score: int = 0
    visual_score: int = 0
    heroes_score: int = 0
    actual_score: int = 0
    drama_score: int = 0
    context_depth_score: int = 0
    universal_score: int = 0
    symbolic_score: int = 0
    viral_score: int = 0
    narrative_score: int = 0
    essence_explain: str = "Анализ по сути не выполнен"
    

    try:

        # --- ВЫЗОВ СЛУЖБЫ DEEPSEEK ---
        essence_result_json = await call_deepseek_api(
            prompt=ESSENCE_FILTRATION,
            text=text_content,
            response_schema=ESSENCE_FILTRATION_SCHEMA,
            model_type='deepseek-chat',
            temperature=0.5,
            tokens=3000
        )
        # -----------------------------

    except Exception as e:
        # Если API-вызов завершился критической ошибкой
        logging.error(f"MsgHandler: Критическая ошибка при сутевом анализе поста ID:{post_id}: {e}")
        # Возвращаем None для filter_initial и сообщение об ошибке
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, f"Критическая ошибка API: {e}"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if essence_result_json:
        essence_score= essence_result_json.get("essence_score", 0)
        essence_explain = essence_result_json.get("essence_explain", "Нет объяснения от ИИ.")

    else:
        essence_score = 0
        essence_explain = "Не удалось выполнить контекстный анализ"
        
    return essence_score, essence_explain
