# handlers/tests/short_term_memory_handlers.py
import asyncio
import random
import logging
from datetime import datetime
from typing import Union, Optional  # Added Optional

from aiogram import Bot, Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
)  # InlineKeyboardMarkup for STM_START_BUTTON
from aiogram.exceptions import TelegramBadRequest
from aiogram.enums import ParseMode  # For HTML parsing

from fsm_states import ShortTermMemoryStates
from settings import (
    STM_WORD_POOL,
    STM_NUM_WORDS_TO_SHOW,
    STM_DISPLAY_DURATION_S,
    STM_RECALL_DURATION_S,
    # STM_HEADERS, # Не используется напрямую здесь, но excel_handler его знает
)

# utils.bot_helpers импортируется в common_handlers, здесь может быть не нужен напрямую,
# если вся логика сохранения/очистки будет вызываться извне или через общую функцию.
# Пока оставим для _safe_delete_message, если понадобится.
from utils.bot_helpers import _safe_delete_message
from keyboards import IKB  # Для STM_START_BUTTON

logger = logging.getLogger(__name__)
router = Router()

STM_INSTRUCTION_TEXT = (
    f"Тест на кратковременную память (слова).\n\n"
    f"Вам будет показано {STM_NUM_WORDS_TO_SHOW} слов на {STM_DISPLAY_DURATION_S} секунд. "
    f"Постарайтесь запомнить как можно больше слов.\n"
    f"Затем у вас будет {STM_RECALL_DURATION_S} секунд, чтобы ввести все слова, которые вы запомнили, одним сообщением.\n\n"
    f"Нажмите 'Начать тест', когда будете готовы."
)
STM_START_BUTTON = InlineKeyboardMarkup(
    inline_keyboard=[
        [IKB(text="Начать тест", callback_data="start_stm_display_words")]
    ]
)


async def start_stm_test(
    trigger_event: Union[Message, CallbackQuery],
    state: FSMContext,
    bot: Bot,
    user_profile: dict,
    battery_context: dict,
):
    """
    Инициирует тест на кратковременную память.
    user_profile: dict с 'unique_id', 'name', 'age'.
    battery_context: dict с 'current_phase', 'sheet_name', 'is_in_battery_mode'.
    """
    chat_id = (
        trigger_event.chat.id
        if isinstance(trigger_event, Message)
        else trigger_event.message.chat.id
    )
    user_id = trigger_event.from_user.id

    await state.set_state(ShortTermMemoryStates.showing_instructions_stm)
    await state.update_data(
        stm_user_profile=user_profile,  # Сохраняем профиль для использования в save_results
        stm_battery_context=battery_context,  # Сохраняем контекст батареи
        stm_chat_id_for_cleanup=chat_id,  # Сохраняем chat_id для cleanup
        stm_test_start_time=datetime.now(),  # Для информации
    )

    # Удаляем предыдущее сообщение (например, "Пауза...") если это колбэк
    if isinstance(trigger_event, CallbackQuery) and trigger_event.message:
        await _safe_delete_message(
            bot,
            chat_id,
            trigger_event.message.message_id,
            "stm_del_prev_pause_msg",
        )

    instruction_message = await bot.send_message(
        chat_id, STM_INSTRUCTION_TEXT, reply_markup=STM_START_BUTTON
    )
    await state.update_data(
        stm_instruction_message_id=instruction_message.message_id
    )
    logger.info(
        f"STM: Инструкции показаны для UID {user_profile.get('unique_id')}"
    )


@router.callback_query(
    F.data == "start_stm_display_words",
    ShortTermMemoryStates.showing_instructions_stm,
)
async def display_words_stm_callback(
    cb: CallbackQuery, state: FSMContext, bot: Bot
):
    await cb.answer()
    data = await state.get_data()
    instruction_msg_id = data.get("stm_instruction_message_id")
    chat_id = cb.message.chat.id

    words_to_show = random.sample(STM_WORD_POOL, STM_NUM_WORDS_TO_SHOW)
    await state.update_data(stm_presented_words=words_to_show)

    words_to_show_str = ", ".join(words_to_show)
    display_text = f"Запомните эти слова:\n\n<b>{words_to_show_str}</b>\n\nОсталось: {STM_DISPLAY_DURATION_S} сек."

    current_message_id_for_timer = None
    if instruction_msg_id:
        try:
            await bot.edit_message_text(
                text=display_text,
                chat_id=chat_id,
                message_id=instruction_msg_id,
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
            current_message_id_for_timer = instruction_msg_id
        except TelegramBadRequest:
            logger.warning(
                "STM: Failed to edit instruction to display words, sending new."
            )
            await _safe_delete_message(
                bot, chat_id, instruction_msg_id, "stm_del_instr_failed_edit"
            )
            new_msg = await bot.send_message(
                chat_id, display_text, parse_mode=ParseMode.HTML
            )
            current_message_id_for_timer = new_msg.message_id
    else:
        new_msg = await bot.send_message(
            chat_id, display_text, parse_mode=ParseMode.HTML
        )
        current_message_id_for_timer = new_msg.message_id

    if current_message_id_for_timer:
        await state.update_data(
            stm_words_display_message_id=current_message_id_for_timer
        )
        await state.set_state(ShortTermMemoryStates.showing_words_stm)
        display_timer_task = asyncio.create_task(
            timer_for_word_display(
                bot, chat_id, current_message_id_for_timer, state
            )
        )
        await state.update_data(stm_display_timer_task=display_timer_task)
        logger.info(
            f"STM: Слова показываются. Таймер запущен. Message ID: {current_message_id_for_timer}"
        )
    else:
        logger.error(
            "STM: Не удалось отправить или отредактировать сообщение для показа слов."
        )
        # Обработка ошибки - возможно, прерывание теста
        await cleanup_stm_test(state, bot, "Ошибка отображения слов.")


async def timer_for_word_display(
    bot: Bot, chat_id: int, message_id: int, state: FSMContext
):
    try:
        for i in range(STM_DISPLAY_DURATION_S - 1, -1, -1):  # Отсчет до 0
            current_fsm_state_str = await state.get_state()
            if (
                not current_fsm_state_str
                or current_fsm_state_str
                != ShortTermMemoryStates.showing_words_stm
            ):
                logger.info(
                    f"STM Display Timer: Прерван. Текущее состояние: {current_fsm_state_str}"
                )
                return

            if i == 0:  # Последний тик, готовимся к смене текста
                break

                # Обновляем сообщение только если есть что обновлять (например, обратный отсчет)
            if (
                i % 10 == 0 or i <= 5
            ):  # Обновлять каждые 10 сек или последние 5
                data = await state.get_data()
                words_str = ", ".join(data.get("stm_presented_words", []))
                edit_text = f"Запомните эти слова:\n\n<b>{words_str}</b>\n\nОсталось: {i} сек."
                try:
                    await bot.edit_message_text(
                        text=edit_text,
                        chat_id=chat_id,
                        message_id=message_id,
                        parse_mode=ParseMode.HTML,
                    )
                except TelegramBadRequest:
                    pass  # Игнорируем, если сообщение не изменилось
            await asyncio.sleep(1)

        # Время вышло, меняем текст на запрос ввода
        recall_prompt_text = (
            f"Время вышло! \nПожалуйста, введите одним сообщением все слова, которые вы запомнили.\n"
            f"У вас {STM_RECALL_DURATION_S} секунд."
        )
        try:
            await bot.edit_message_text(
                text=recall_prompt_text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=None,
            )
            await state.update_data(stm_recall_prompt_message_id=message_id)
            logger.info(f"STM: Запрос на ввод слов. Message ID: {message_id}")
        except TelegramBadRequest:
            logger.warning(
                "STM: Failed to edit display to recall prompt, sending new."
            )
            await _safe_delete_message(
                bot, chat_id, message_id, "stm_del_display_failed_edit"
            )
            new_prompt_msg = await bot.send_message(
                chat_id, recall_prompt_text
            )
            await state.update_data(
                stm_recall_prompt_message_id=new_prompt_msg.message_id
            )
            logger.info(
                f"STM: Запрос на ввод слов (новое сообщение). Message ID: {new_prompt_msg.message_id}"
            )

        await state.set_state(ShortTermMemoryStates.waiting_for_recall_stm)
        recall_task = asyncio.create_task(
            timer_for_recall(bot, chat_id, state)
        )
        await state.update_data(stm_recall_timer_task=recall_task)

    except asyncio.CancelledError:
        logger.info("STM Display Timer: Был отменен (вероятно, /stopbattery).")
    except Exception as e:
        logger.error(f"STM Display Timer: Ошибка: {e}", exc_info=True)
        await cleanup_stm_test(
            state, bot, "Ошибка в таймере отображения слов."
        )


async def timer_for_recall(bot: Bot, chat_id: int, state: FSMContext):
    try:
        await asyncio.sleep(STM_RECALL_DURATION_S)
        current_fsm_state_str = await state.get_state()
        if (
            current_fsm_state_str
            == ShortTermMemoryStates.waiting_for_recall_stm
        ):
            logger.info(
                f"STM Recall Timer: Время на ввод слов истекло для чата {chat_id}."
            )
            data = await state.get_data()
            recall_prompt_msg_id = data.get("stm_recall_prompt_message_id")

            timeout_text = "Время на ввод слов истекло."
            if recall_prompt_msg_id:
                try:
                    await bot.edit_message_text(
                        text=timeout_text,
                        chat_id=chat_id,
                        message_id=recall_prompt_msg_id,
                        reply_markup=None,
                    )
                except TelegramBadRequest:
                    await _safe_delete_message(
                        bot,
                        chat_id,
                        recall_prompt_msg_id,
                        "stm_del_recall_prompt_timeout_edit_fail",
                    )
                    # await bot.send_message(chat_id, timeout_text) # Не обязательно, т.к. сразу сохраняем
            # else:
            #    await bot.send_message(chat_id, timeout_text)

            await state.update_data(
                stm_recalled_words_input="N/A (таймаут)",
                stm_interrupted_by_timeout=True,
            )
            await save_stm_results_internal(
                state, is_interrupted_by_timeout=True
            )
            await cleanup_stm_test(
                state, bot, final_text="Тест завершен (время на ввод вышло)."
            )
    except asyncio.CancelledError:
        logger.info(
            "STM Recall Timer: Был отменен (ввод пользователя или /stopbattery)."
        )
    except Exception as e:
        logger.error(f"STM Recall Timer: Ошибка: {e}", exc_info=True)
        await cleanup_stm_test(state, bot, "Ошибка в таймере ввода слов.")


@router.message(ShortTermMemoryStates.waiting_for_recall_stm)
async def process_recalled_words_stm(
    message: Message, state: FSMContext, bot: Bot
):
    user_recalled_words = message.text
    await _safe_delete_message(
        bot,
        message.chat.id,
        message.message_id,
        "stm_user_recalled_words_input",
    )

    data = await state.get_data()
    recall_timer_task = data.get("stm_recall_timer_task")
    if recall_timer_task and not recall_timer_task.done():
        recall_timer_task.cancel()
        logger.info(
            "STM: Таймер на ввод ответа отменен из-за ввода пользователя."
        )

    await state.update_data(
        stm_recalled_words_input=user_recalled_words,
        stm_interrupted_by_timeout=False,
    )

    recall_prompt_msg_id = data.get("stm_recall_prompt_message_id")
    confirmation_text = "Ваши слова приняты."  # Короткое подтверждение
    if recall_prompt_msg_id:
        try:
            await bot.edit_message_text(
                text=confirmation_text,
                chat_id=message.chat.id,
                message_id=recall_prompt_msg_id,
                reply_markup=None,
            )
        except TelegramBadRequest:
            await _safe_delete_message(
                bot,
                message.chat.id,
                recall_prompt_msg_id,
                "stm_del_recall_prompt_user_input_edit_fail",
            )

    await save_stm_results_internal(state)
    await cleanup_stm_test(
        state, bot, final_text="Тест на кратковременную память завершен."
    )


async def save_stm_results_internal(
    state: FSMContext,
    is_interrupted_by_command: bool = False,
    is_interrupted_by_timeout: bool = False,
):
    """Внутренняя функция для сохранения результатов STM. Вызывается из обработчиков или при прерывании."""
    data = await state.get_data()
    user_profile = data.get("stm_user_profile")
    battery_context = data.get("stm_battery_context")

    if not user_profile or not battery_context:
        logger.error(
            "STM Save: Отсутствуют данные профиля или контекста батареи. Сохранение невозможно."
        )
        return

    uid = user_profile.get("unique_id")
    sheet_name = battery_context.get("sheet_name")

    if not uid or not sheet_name:
        logger.error(
            f"STM Save: UID ({uid}) или sheet_name ({sheet_name}) не определены. Сохранение невозможно."
        )
        return

    presented_words_list = data.get("stm_presented_words", [])
    presented_words_str = ", ".join(presented_words_list)
    recalled_words_input_str = data.get(
        "stm_recalled_words_input", "N/A (не введено)"
    )

    correct_count = 0
    # Формируем строку для сохранения, даже если это "N/A"
    final_recalled_words_for_excel = recalled_words_input_str

    if (
        isinstance(recalled_words_input_str, str)
        and recalled_words_input_str
        not in ["N/A (таймаут)", "N/A (не введено)", "N/A (прервано)"]
        and presented_words_list
    ):

        # Исправленная логика для recalled_set:
        # Сначала получаем список слов из строки, потом уже set
        processed_recalled_list = []
        # Разделяем по пробелам и запятым, удаляем пустые строки после strip
        potential_words = recalled_words_input_str.replace(',', ' ').split()
        for potential_word in potential_words:
            cleaned_word = potential_word.strip().lower()
            if cleaned_word:  # Добавляем только непустые слова
                processed_recalled_list.append(cleaned_word)

        recalled_set = set(processed_recalled_list)
        presented_set = set(word.lower() for word in presented_words_list)
        correct_count = len(recalled_set.intersection(presented_set))
        # Можно также сохранить очищенный и соединенный список recalled_set для консистентности,
        # но в ТЗ было "слова пользователя, как есть" для recalled_words_str.
        # Оставляем recalled_words_input_str как есть для поля STM_Recalled_Words.

    results_data = {
        "STM_Presented_Words": presented_words_str,
        "STM_Recalled_Words": final_recalled_words_for_excel,  # Используем исходный ввод или N/A
        "STM_Correct_Count": correct_count,
        "STM_Presented_Count": len(presented_words_list),
        "STM_Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "STM_Interrupted": is_interrupted_by_command
        or is_interrupted_by_timeout
        or data.get("stm_interrupted_by_timeout", False),
    }

    logger.info(
        f"STM: Подготовка к сохранению результатов для UID {uid} в лист '{sheet_name}': {results_data}"
    )

    from utils.excel_handler import (
        append_results_to_excel,
    )  # Импорт здесь, чтобы избежать циклических зависимостей на верхнем уровне

    try:
        # user_profile_data содержит UID, Name, Age
        await asyncio.to_thread(
            append_results_to_excel, user_profile, results_data, sheet_name
        )
        logger.info(
            f"STM: Результаты для UID {uid} успешно сохранены в лист '{sheet_name}'."
        )
    except Exception as e:
        logger.error(
            f"STM: Ошибка при сохранении результатов для UID {uid} в лист '{sheet_name}': {e}",
            exc_info=True,
        )
        # Уведомление пользователя об ошибке сохранения (можно сделать через bot объект, если он доступен)
        # chat_id = data.get("stm_chat_id_for_cleanup")
        # if chat_id and bot_instance: # bot_instance нужно передать
        #     await bot_instance.send_message(chat_id, "Произошла ошибка при сохранении ваших STM результатов.")


async def cleanup_stm_test(
    state: FSMContext,
    bot: Bot,
    final_text: Optional[str] = None,
    is_stopped_by_battery: bool = False,
):
    """Очищает UI теста STM и FSM состояние STM."""
    logger.info(
        f"STM Cleanup: Начало очистки. Final text: '{final_text}', Stopped by battery: {is_stopped_by_battery}"
    )
    data = await state.get_data()
    chat_id = data.get("stm_chat_id_for_cleanup")

    # Отменяем активные таймеры, если они есть
    display_timer_task = data.get("stm_display_timer_task")
    if display_timer_task and not display_timer_task.done():
        display_timer_task.cancel()
        logger.info("STM Cleanup: Display timer task отменен.")

    recall_timer_task = data.get("stm_recall_timer_task")
    if recall_timer_task and not recall_timer_task.done():
        recall_timer_task.cancel()
        logger.info("STM Cleanup: Recall timer task отменен.")

    # Удаляем или редактируем сообщения
    ids_to_try_edit_then_delete = [
        data.get("stm_instruction_message_id"),
        data.get("stm_words_display_message_id"),
        data.get("stm_recall_prompt_message_id"),
    ]
    # Фильтруем None и делаем уникальными, сохраняя порядок для попытки редактирования последнего
    unique_ids = []
    for msg_id in reversed(
        ids_to_try_edit_then_delete
    ):  # Начинаем с последнего (наиболее вероятного для редактирования)
        if msg_id and msg_id not in unique_ids:
            unique_ids.insert(0, msg_id)

    edited_successfully = False
    if final_text and chat_id and unique_ids:
        last_known_message_id = unique_ids[
            -1
        ]  # Пытаемся отредактировать последнее известное сообщение
        try:
            await bot.edit_message_text(
                text=final_text,
                chat_id=chat_id,
                message_id=last_known_message_id,
                reply_markup=None,
            )
            logger.info(
                f"STM Cleanup: Сообщение {last_known_message_id} отредактировано на '{final_text}'."
            )
            edited_successfully = True
            # Удаляем остальные, если они не совпадают
            for msg_id in unique_ids[:-1]:
                await _safe_delete_message(
                    bot, chat_id, msg_id, "stm_cleanup_other_msgs"
                )
        except TelegramBadRequest:
            logger.warning(
                f"STM Cleanup: Не удалось отредактировать сообщение {last_known_message_id}. Удаляем все и отправляем новое."
            )
            for msg_id in unique_ids:
                await _safe_delete_message(
                    bot, chat_id, msg_id, "stm_cleanup_edit_fail_del_all"
                )
            if (
                not is_stopped_by_battery
            ):  # Если батарея не была остановлена (и не покажет свое сообщение)
                await bot.send_message(
                    chat_id, final_text
                )  # Отправляем final_text новым сообщением
            edited_successfully = (
                False  # Технически не отредактировано, а заменено
            )
    elif chat_id and unique_ids:  # Нет final_text, просто удаляем все
        for msg_id in unique_ids:
            await _safe_delete_message(
                bot, chat_id, msg_id, "stm_cleanup_no_final_text"
            )

    # Если final_text был, но не было сообщений для редактирования, и батарея не остановлена
    if (
        final_text
        and not edited_successfully
        and not unique_ids
        and chat_id
        and not is_stopped_by_battery
    ):
        await bot.send_message(chat_id, final_text)

    # Очистка только STM-специфичных данных из FSM. Контекст батареи остается.
    # Состояние FSM будет сброшено или изменено BatteryManager'ом.
    current_data = await state.get_data()
    new_data = {
        k: v for k, v in current_data.items() if not k.startswith("stm_")
    }
    new_data['last_stm_cleaned_up'] = True  # Флаг для Battery Manager
    await state.set_data(new_data)

    # Важно! Состояние STM должно быть сброшено, чтобы BatteryManager мог перейти к следующему шагу.
    # BatteryManager будет ожидать, что состояние либо None, либо специфическое состояние батареи.
    # Мы НЕ используем state.clear() здесь, так как это сотрет контекст батареи.
    # Вместо этого, BatteryManager должен сам перевести состояние после завершения cleanup.
    # Для этого cleanup может возвращать флаг или BatteryManager будет проверять состояние.
    # Пока что, вызывающий код (в BatteryManager) должен будет установить следующее состояние.
    # Либо, если мы хотим, чтобы этот хэндлер сам "вышел" из своего FSM, не затрагивая FSM батареи:
    current_fsm_state_name = await state.get_state()
    if current_fsm_state_name and current_fsm_state_name.startswith(
        "ShortTermMemoryStates"
    ):
        await state.set_state(
            None
        )  # Выходим из FSM теста, возвращая управление
        logger.info("STM Cleanup: Состояние FSM теста STM сброшено на None.")
