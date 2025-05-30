# settings.py
import os

# --- Excel Settings ---
EXCEL_FILENAME = "persistent_user_data.xlsx"

BASE_HEADERS = ["Telegram ID", "Unique ID", "Name", "Age"]
CORSI_HEADERS = [
    "Corsi - Max Correct Sequence Length",
    "Corsi - Avg Time Per Element (s)",
    "Corsi - Sequence Times Detail",
    "Corsi - Interrupted",
]
STROOP_HEADERS = [
    "Stroop Part1 Time (s)",
    "Stroop Part1 Errors",
    "Stroop Part2 Time (s)",
    "Stroop Part2 Errors",
    "Stroop Part3 Time (s)",
    "Stroop Part3 Errors",
    "Stroop - Interrupted",
]
REACTION_TIME_HEADERS = [
    "ReactionTime_Time_ms",
    "ReactionTime_Attempts",
    "ReactionTime_Status",
    "ReactionTime_Interrupted",
]
VERBAL_FLUENCY_HEADERS = [
    "VerbalFluency_Category",
    "VerbalFluency_Letter",
    "VerbalFluency_WordCount",
    "VerbalFluency_WordsList",
    "VerbalFluency_Interrupted",
]
MENTAL_ROTATION_HEADERS = [
    "MentalRotation_CorrectAnswers",
    "MentalRotation_AverageReactionTime_s",
    "MentalRotation_TotalTime_s",
    "MentalRotation_IndividualResponses",
    "MentalRotation_Interrupted",
]
RAVEN_MATRICES_HEADERS = [
    "RavenMatrices_CorrectAnswers",
    "RavenMatrices_TotalTime_s",
    "RavenMatrices_AvgTimeCorrect_s",
    "RavenMatrices_IndividualTimes_s",
    "RavenMatrices_Interrupted",
]

ALL_EXPECTED_HEADERS = (
    BASE_HEADERS
    + CORSI_HEADERS
    + STROOP_HEADERS
    + REACTION_TIME_HEADERS
    + VERBAL_FLUENCY_HEADERS
    + MENTAL_ROTATION_HEADERS
    + RAVEN_MATRICES_HEADERS
)

# --- Stroop Test Constants ---
STROOP_COLORS_DEF = {
    "Красный": {"rgb": (220, 20, 60), "name": "КРАСНЫЙ", "emoji": "🟥"},
    "Синий": {"rgb": (0, 0, 205), "name": "СИНИЙ", "emoji": "🟦"},
    "Зеленый": {"rgb": (34, 139, 34), "name": "ЗЕЛЕНЫЙ", "emoji": "🟩"},
    "Желтый": {"rgb": (255, 215, 0), "name": "ЖЕЛТЫЙ", "emoji": "🟨"},
    "Черный": {"rgb": (0, 0, 0), "name": "ЧЕРНЫЙ", "emoji": "⬛"},
}
STROOP_COLOR_NAMES = list(STROOP_COLORS_DEF.keys())
STROOP_ITERATIONS_PER_PART = 6
STROOP_FONT_PATH = "arial.ttf"  # Make sure this font is available
STROOP_IMAGE_SIZE = (300, 150)
STROOP_TEXT_COLOR_ON_PATCH = (255, 255, 255)
STROOP_INSTRUCTION_TEXT_PART1 = (
    "Добро пожаловать в <b>Тест Струпа!</b>\n\n"
    "Этот тест оценивает вашу способность подавлять когнитивную"
    " интерференцию. Он состоит из трех частей.\n\n"
    "<b>Часть 1: Слова</b>\n"
    "Вам будут показаны названия цветов, написанные черным жирным шрифтом."
    " Ваша задача – как можно быстрее нажать на <b>цветной квадрат</b>"
    " (кнопку-эмодзи), соответствующий <b>написанному названию"
    " цвета</b>.\n\n"
    "Приготовьтесь. Нажмите 'Понятно', чтобы начать Часть 1."
)
STROOP_INSTRUCTION_TEXT_PART2 = (
    "<b>Часть 2: Цветные Плашки</b>\n"
    "Теперь вам будут показаны цветные прямоугольники. На каждом"
    " прямоугольнике белыми буквами будет написано случайное название цвета"
    " (оно не имеет значения для задачи).\n\n"
    "Ваша задача – как можно быстрее нажать на кнопку с <b>названием"
    " цвета</b>, соответствующим <b>цвету самого прямоугольника"
    " (фона)</b>.\n\n"
    "Приготовьтесь. Нажмите 'Понятно', чтобы начать Часть 2."
)
STROOP_INSTRUCTION_TEXT_PART3 = (
    "<b>Часть 3: Интерференция</b>\n"
    "В этой части вам снова будут показаны слова, обозначающие цвета."
    " Однако теперь сами слова будут написаны <b>цветными чернилами</b>,"
    " причем цвет чернил НЕ будет совпадать со значением слова.\n\n"
    "Ваша задача – как можно быстрее нажать на кнопку с <b>названием"
    " цвета</b>, соответствующим <b>цвету чернил</b>, которым написано"
    " слово (игнорируйте значение слова).\n\n"
    "Приготовьтесь. Нажмите 'Понятно', чтобы начать Часть 3."
)


# --- Reaction Time Test Constants ---
# Путь к директории с изображениями для RT
RT_IMAGES_DIR = os.path.join("images", "rt_images")
# REACTION_TIME_IMAGE_POOL будет заполняться в main_bot.py
REACTION_TIME_IMAGE_POOL: list[str] = []
REACTION_TIME_MEMORIZATION_S = 10
REACTION_TIME_STIMULUS_INTERVAL_S = 6
REACTION_TIME_MAX_ATTEMPTS = 2
REACTION_TIME_NUM_STIMULI_IN_SEQUENCE = 7
REACTION_TIME_TARGET_REACTION_WINDOW_S = REACTION_TIME_STIMULUS_INTERVAL_S - 1


# --- Verbal Fluency Test Constants ---
VERBAL_FLUENCY_DURATION_S = 60
VERBAL_FLUENCY_CATEGORY = "Общие слова"
_USABLE_RUSSIAN_LETTERS_VF = "АБВГДЕЖЗИКЛМНОПРСТУФХЦЧШЭЯ"
VERBAL_FLUENCY_TASK_POOL: list[dict] = []
if _USABLE_RUSSIAN_LETTERS_VF:
    for letter_vf in _USABLE_RUSSIAN_LETTERS_VF:
        VERBAL_FLUENCY_TASK_POOL.append(
            {
                "base_category": VERBAL_FLUENCY_CATEGORY,
                "letter": letter_vf.upper(),
            }
        )

# --- Mental Rotation Test Constants ---
MENTAL_ROTATION_NUM_ITERATIONS = 5
MR_BASE_DIR = os.path.join("images", "mental_rotation")
MR_REFERENCES_DIR = os.path.join(MR_BASE_DIR, "references")
MR_CORRECT_PROJECTIONS_DIR = os.path.join(MR_BASE_DIR, "correct_projections")
MR_DISTRACTORS_DIR = os.path.join(MR_BASE_DIR, "distractors")

# Эти списки будут заполнены в main_bot.py при инициализации
MR_REFERENCE_FILES: list[str] = []
MR_CORRECT_PROJECTIONS_MAP: dict[str, list[str]] = {}
MR_ALL_DISTRACTORS_FILES: list[str] = []

MR_COLLAGE_CELL_SIZE = (250, 250)
MR_COLLAGE_BG_COLOR = (255, 255, 255)
MR_FEEDBACK_DISPLAY_TIME_S = 0.75


# --- Raven Matrices Test Constants ---
RAVEN_NUM_TASKS_TO_PRESENT = 20
RAVEN_TOTAL_AVAILABLE_TASKS_IDEAL = 80
RAVEN_BASE_DIR = os.path.join("images", "raven_matrices")
RAVEN_FEEDBACK_DISPLAY_TIME_S = 0.75
# RAVEN_ALL_TASK_FILES будет заполнен в main_bot.py
RAVEN_ALL_TASK_FILES: list[str] = []


