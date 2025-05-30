# fsm_states.py
from aiogram.fsm.state import StatesGroup, State


class UserData(StatesGroup):
    waiting_for_first_time_response = State()
    waiting_for_name = State()
    waiting_for_age = State()
    waiting_for_unique_id = State()
    waiting_for_test_overwrite_confirmation = State()


class CorsiTestStates(StatesGroup):
    showing_sequence = State()
    waiting_for_user_sequence = State()


class StroopTestStates(StatesGroup):
    initial_instructions = State()
    part1_stimulus_response = State()
    part2_instructions = State()
    part2_stimulus_response = State()
    part3_instructions = State()
    part3_stimulus_response = State()


class ReactionTimeTestStates(StatesGroup):
    initial_instructions = State()
    memorization_display = State()
    reaction_stimulus_display = State()
    awaiting_retry_confirmation = State()


class VerbalFluencyStates(StatesGroup):
    showing_instructions_and_task = State()
    collecting_words = State()


class MentalRotationStates(StatesGroup):
    initial_instructions_mr = State()
    displaying_stimulus_mr = State()
    processing_answer_mr = State()
    inter_iteration_countdown_mr = State()


class RavenMatricesStates(StatesGroup):
    initial_instructions_raven = State()
    displaying_task_raven = State()
    processing_feedback_raven = State()


