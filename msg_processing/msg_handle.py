import asyncio
import logging
from typing import Tuple, Dict, Any
# Импортируем промпт и схему
from prompts import FILTER_INITIAL, FILTER_INITIAL_SCHEMA, CONTEXT_FILTRATION, CONTEXT_FILTRATION_SCHEMA, ESSENCE_FILTRATION, ESSENCE_FILTRATION_SCHEMA
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
    filter_initial: bool = True
    filter_initial_explain: str = "Анализ не выполнен"
    context_score: float = 0.0
    context_explain: str = "Контекстный анализ не выполнен"
    

    try:

        # --- ВЫЗОВ СЛУЖБЫ DEEPSEEK для первичной фильтрации ---
        filter_initial_result_json = await call_deepseek_api(
            prompt=FILTER_INITIAL,
            text=text_content,
            response_schema=FILTER_INITIAL_SCHEMA,
            model_type='deepseek-chat',
            temperature=0.0
        )
        # -----------------------------

    except Exception as e:
        # Если API-вызов завершился критической ошибкой
        logging.error(f"MsgHandler: Критическая ошибка при формальном анализе поста ID:{post_id}: {e}")
        # Возвращаем None для filter_initial и сообщение об ошибке
        return False, f"Критическая ошибка API: {e}", 0.0, "Ошибка при анализе контекста"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if filter_initial_result_json:

        filter_initial = filter_initial_result_json.get("filter", False)
        filter_initial_explain = filter_initial_result_json.get("explain", "Нет объяснения от ИИ.")
        
        # --- ОБРАБОТКА КОНТЕКСТА ---
        # Если фильтрация false, то context тоже false

        if not filter_initial:
            context_score = 0.0
            context_explain = "запретная тема"
        else:

            # Если фильтрация прошла успешно, выполняем контекстный анализ
            try:
                context_result_json = await call_deepseek_api(
                    prompt=CONTEXT_FILTRATION,
                    text=text_content,
                    response_schema=CONTEXT_FILTRATION_SCHEMA,
                    model_type='deepseek-chat',
                    temperature=0.3,
                    tokens=500
                )
                
                if context_result_json:
                    # Для контекста используем булево значение на основе порога (например, score > 5)
                    context_score = context_result_json.get("context_score", 0)
                    context_explain = context_result_json.get("context_explain", "Нет объяснения контекста")
                else:
                    context_score = 0.0
                    context_explain = "Не удалось выполнить контекстный анализ"
                    
            except Exception as context_error:
                logging.error(f"MsgHandler: Ошибка при контекстном анализе поста ID:{post_id}: {context_error}")
                context_score = 0.0
                context_explain = f"Ошибка контекстного анализа: {context_error}"        
    
    else:
        logging.warning(f"MsgHandler: Deepseek вернул пустой ответ для поста ID:{post_id}.")
        filter_initial = False
        filter_initial_explain = "Невалидный или пустой ответ от ИИ"
        context_score = 0.0
        context_explain = "Невозможно проанализировать контекст из-за ошибки фильтрации"

    # Возвращаем требуемую пару
    return filter_initial, filter_initial_explain, context_score, context_explain

async def process_message_by_essence(post_id: int, text_content: str) -> Tuple[float, float, str]:

    # Инициализируем значения по умолчанию
    essence_score: float = 0.0
    essence_max: float = 10.0
    essence_explain: str = "Анализ по сути не выполнен"
    

    try:

        # --- ВЫЗОВ СЛУЖБЫ DEEPSEEK ---
        essence_result_json = await call_deepseek_api(
            prompt=ESSENCE_FILTRATION,
            text=text_content,
            response_schema=ESSENCE_FILTRATION_SCHEMA,
            model_type='deepseek-chat',
            temperature=0.6,
            tokens=3000
        )
        # -----------------------------

    except Exception as e:
        # Если API-вызов завершился критической ошибкой
        logging.error(f"MsgHandler: Критическая ошибка при сутевом анализе поста ID:{post_id}: {e}")
        # Возвращаем None для filter_initial и сообщение об ошибке
        return 0.0, f"Критическая ошибка API: {e}"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if essence_result_json:
        essence_score = essence_result_json.get("essence_score", 0)
        essence_max = essence_result_json.get("essence_max", 0)
        essence_explain = essence_result_json.get("essence_explain", "Нет объяснения от ИИ.")
    else:
        essence_score = 0.0
        essence_max = 0.0
        essence_explain = "Не удалось выполнить контекстный анализ"
        
    return essence_score, essence_max, essence_explain
