from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Главное меню
main_inkb = InlineKeyboardMarkup(row_width=1)
portfolio_option = InlineKeyboardButton(
    text="Собрать портфель", callback_data='assemble_portfolio')
# my_portfolio_option = InlineKeyboardButton(
#     text="Мой портфель", callback_data='check_portfolio')
# main_inkb.add(portfolio_option, my_portfolio_option)
main_inkb.add(portfolio_option)

# Клавиатура для выбора уровня риска
risk_level_keyboard = InlineKeyboardMarkup(row_width=1)
low_risk = InlineKeyboardButton(text="Низкий", callback_data='low_risk')
medium_risk = InlineKeyboardButton(text="Средний", callback_data='medium_risk')
high_risk = InlineKeyboardButton(text="Высокий", callback_data='high_risk')
risk_level_keyboard.add(low_risk, medium_risk, high_risk)

# Клавиатура для выбора параметра оптимизации
optimization_param_keyboard = InlineKeyboardMarkup(row_width=1)
risk_param = InlineKeyboardButton(text="Риск", callback_data='risk')
profitability_param = InlineKeyboardButton(
    text="Доходность", callback_data='return')
liquidity_param = InlineKeyboardButton(
    text="Ликвидность", callback_data='liquidity')
optimization_param_keyboard.add(
    risk_param, profitability_param, liquidity_param)

# Клавиатура для выбора метрики ликвидности
liquidity_param_keyboard = InlineKeyboardMarkup(row_width=1)
avg_trading_volume = InlineKeyboardButton(
    text="Средний торговый объем", callback_data='Average Trading Volume')
turnover_ratio = InlineKeyboardButton(
    text="Коэффициент оборота", callback_data='Turnover Ratio')
bid_ask_spread = InlineKeyboardButton(
    text="Спред между спросом и предложением", callback_data='Bid-Ask Spread')
time_to_sale = InlineKeyboardButton(
    text="Время до продажи", callback_data='Time to Sale')
liquidity_param_keyboard.add(
    avg_trading_volume, turnover_ratio, bid_ask_spread, time_to_sale)

# Кнопка отмены
cancel_button = InlineKeyboardButton(text="Отмена", callback_data='cancel')
cancel_inkb = InlineKeyboardMarkup().add(cancel_button)

# Клавиатура для да/нет ответа
yes_no_keyboard = InlineKeyboardMarkup(row_width=1)
yes_button = InlineKeyboardButton(text="Да", callback_data='yes')
no_button = InlineKeyboardButton(text="Нет", callback_data='no')
yes_no_keyboard.add(yes_button, no_button)
