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

    subject_score = 0
    object_score = 0
    which_score = 0
    action_score = 0
    time_place_score = 0
    how_score = 0
    reason_score = 0
    consequences_score = 0
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
        return False, f"Критическая ошибка API: {e}", 0, 0, 0, 0, 0, 0, 0, 0, "Ошибка при анализе контекста"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if filter_initial_result_json:

        filter_initial = filter_initial_result_json.get("filter", False)
        filter_initial_explain = filter_initial_result_json.get("explain", "Нет объяснения от ИИ.")
        
        # --- ОБРАБОТКА КОНТЕКСТА ---
        # Если фильтрация false, то context тоже false

        if not filter_initial:

            subject_score = 0
            object_score = 0
            which_score = 0
            action_score = 0
            time_place_score = 0
            how_score = 0
            reason_score = 0
            consequences_score = 0
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

                    subject_score = context_result_json.get("subject_score", 0)
                    object_score = context_result_json.get("object_score", 0)
                    which_score = context_result_json.get("which_score", 0)
                    action_score = context_result_json.get("action_score", 0)
                    time_place_score = context_result_json.get("time_place_score", 0)
                    how_score = context_result_json.get("how_score", 0)
                    reason_score = context_result_json.get("reason_score", 0)
                    consequences_score = context_result_json.get("consequences_score", 0)

                    context_explain = context_result_json.get("context_explain", "Нет объяснения контекста")

                else:

                    subject_score = 0
                    object_score = 0
                    which_score = 0
                    action_score = 0
                    time_place_score = 0
                    how_score = 0
                    reason_score = 0
                    consequences_score = 0
                    context_explain = "Не удалось выполнить контекстный анализ"
                    
            except Exception as context_error:
                logging.error(f"MsgHandler: Ошибка при контекстном анализе поста ID:{post_id}: {context_error}")

                subject_score = 0
                object_score = 0
                which_score = 0
                action_score = 0
                time_place_score = 0
                how_score = 0
                reason_score = 0
                consequences_score = 0
                context_explain = f"Ошибка контекстного анализа: {context_error}"        
    
    else:
        logging.warning(f"MsgHandler: Deepseek вернул пустой ответ для поста ID:{post_id}.")
        filter_initial = False
        filter_initial_explain = "Невалидный или пустой ответ от ИИ"

        subject_score = 0
        object_score = 0
        which_score = 0
        action_score = 0
        time_place_score = 0
        how_score = 0
        reason_score = 0
        consequences_score = 0
        context_explain = "Невозможно проанализировать контекст из-за ошибки фильтрации"

    # Возвращаем требуемую пару
    return filter_initial, filter_initial_explain, subject_score, object_score, which_score, action_score, time_place_score, how_score, reason_score, consequences_score, context_explain




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
    social_score: int = 0
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
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, f"Критическая ошибка API: {e}"

    # --- ПАРСИНГ РЕЗУЛЬТАТОВ ---
    if essence_result_json:

        emotional_score = essence_result_json.get("emotional_score", 0)
        visual_score = essence_result_json.get("visual_score", 0)
        heroes_score = essence_result_json.get("heroes_score", 0)
        actual_score = essence_result_json.get("actual_score", 0)
        drama_score = essence_result_json.get("drama_score", 0)
        context_depth_score = essence_result_json.get("context_depth_score", 0)
        universal_score = essence_result_json.get("universal_score", 0)
        symbolic_score = essence_result_json.get("symbolic_score", 0)
        viral_score = essence_result_json.get("viral_score", 0)
        social_score= essence_result_json.get("social_score", 0)

        essence_explain = essence_result_json.get("essence_explain", "Нет объяснения от ИИ.")

    else:

        emotional_score = 0
        visual_score = 0
        heroes_score = 0
        actual_score = 0
        drama_score = 0
        context_depth_score = 0
        universal_score = 0
        symbolic_score = 0
        viral_score = 0
        social_score = 0
        essence_explain = "Не удалось выполнить контекстный анализ"
        
    return emotional_score, visual_score, heroes_score, actual_score, drama_score, context_depth_score, universal_score, symbolic_score, viral_score, social_score, essence_explain
