import os
import json
import numpy as np
import matplotlib.pyplot as plt
import asyncio
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.filters import Text

from loader import bot, dp
from keyboards.user_kb import (
    risk_level_keyboard, optimization_param_keyboard, main_inkb,
    cancel_inkb, yes_no_keyboard, liquidity_param_keyboard)
from database.database import user_db
from model import model

risk_stats = {'low_risk': 0.05,
              'medium_risk': 0.12,
              'high_risk': 0.20}


class PortfolioStates(StatesGroup):
    RISK_PROFILE = State()
    RISK_LEVEL = State()
    OPTIMIZATION_PARAM = State()
    LIQUIDITY_PARAM = State()
    RETURN_INPUT = State()
    PORTFOLIO_ASSEMBLED = State()


@dp.message_handler(commands=['start'])
async def start_portfolio_assembly(message: types.Message, state: FSMContext):
    await message.answer("Привет! Я финансовый консультант, могу помочь тебе составить оптимальный портфель из ПИФов недвижимости! Чем могу помочь сегодня?",
                         reply_markup=main_inkb)


@dp.callback_query_handler(lambda callback_query: callback_query.data == "assemble_portfolio")
async def assemble_portfolio(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    has_risk_profile = await user_db.user_has_risk_profile(user_id)

    if not has_risk_profile:
        await bot.edit_message_reply_markup(callback_query.from_user.id, callback_query.message.message_id, reply_markup=None)
        await callback_query.message.answer("Выберите уровень риска:", reply_markup=risk_level_keyboard)
        await PortfolioStates.RISK_LEVEL.set()
    else:
        await bot.edit_message_reply_markup(callback_query.from_user.id, callback_query.message.message_id, reply_markup=None)
        await callback_query.message.answer("Выберите параметр, по которому хотите оптимизировать портфель:",
                                            reply_markup=optimization_param_keyboard)
        await PortfolioStates.OPTIMIZATION_PARAM.set()


@dp.callback_query_handler(state=PortfolioStates.RISK_LEVEL)
async def choose_risk_level(callback_query: types.CallbackQuery, state: FSMContext):
    # await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
    async with state.proxy() as data:
        data['risk_profile'] = callback_query.data
        data['target_risk'] = risk_stats[callback_query.data]

    await user_db.save_user(callback_query.from_user.id, callback_query.message.from_user.full_name,
                            callback_query.message.from_user.username, callback_query.data, False)

    await bot.edit_message_text("Выберите параметр, по которому хотите оптимизировать портфель:",
                                callback_query.from_user.id, callback_query.message.message_id,
                                reply_markup=optimization_param_keyboard)
    await PortfolioStates.OPTIMIZATION_PARAM.set()


@dp.callback_query_handler(state=PortfolioStates.OPTIMIZATION_PARAM)
async def choose_optimization_param(callback_query: types.CallbackQuery, state: FSMContext):
    param = callback_query.data

    async with state.proxy() as data:
        data['optimization_param'] = param

    if param == 'liquidity':
        await callback_query.message.answer("Выберите метрику ликвидности:", reply_markup=liquidity_param_keyboard)
        await PortfolioStates.LIQUIDITY_PARAM.set()
    elif param == 'return':
        await callback_query.message.answer("Введите значение доходности в процентах:")
        await PortfolioStates.RETURN_INPUT.set()
    else:
        await PortfolioStates.PORTFOLIO_ASSEMBLED.set()
        await assemble_optimized_portfolio(callback_query.message, state)


@dp.callback_query_handler(state=PortfolioStates.LIQUIDITY_PARAM)
async def choose_liquidity_param(callback_query: types.CallbackQuery, state: FSMContext):
    liquidity_metric = callback_query.data

    async with state.proxy() as data:
        data['liquidity_metric'] = liquidity_metric

    await PortfolioStates.PORTFOLIO_ASSEMBLED.set()
    await assemble_optimized_portfolio(callback_query.message, state)


@dp.message_handler(state=PortfolioStates.RETURN_INPUT)
async def input_return(message: types.Message, state: FSMContext):
    try:
        target_return = float(message.text)
        if not (0 <= target_return <= 100):
            raise ValueError("Доходность должна быть в пределах от 0 до 100")
    except ValueError as e:
        await message.answer(f"Некорректное значение доходности: {e}. Пожалуйста, введите значение доходности в процентах:")
        return

    async with state.proxy() as data:
        data['target_return'] = target_return / 100

    await PortfolioStates.PORTFOLIO_ASSEMBLED.set()
    await assemble_optimized_portfolio(message, state)


@dp.callback_query_handler(state=PortfolioStates.PORTFOLIO_ASSEMBLED)
async def assemble_optimized_portfolio(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        optimization_param = data.get('optimization_param')
        target_risk = data.get('risk')
        target_return = data.get('target_return')
        liquidity_metric = data.get(
            'liquidity_metric') or 'Average Trading Volume'

    portfolio_stats, returns = await model.model(optimization_goal=optimization_param, target_risk=target_risk,
                                                 target_return=target_return, liquidity_metric=liquidity_metric)

    weights = portfolio_stats['weights']
    expected_return = portfolio_stats['expected_return']
    expected_volatility = portfolio_stats['expected_volatility']
    sharpe_ratio = portfolio_stats['sharpe_ratio']

    response = (
        "Оптимальные веса портфеля:\n"
        + "\n".join([f"{asset}: {weight*100:.2f}%" for asset,
                    weight in weights.items()]) + "\n\n"
        + f"Ожидаемая доходность: {expected_return * 100:.2f}%\n"
        + f"Ожидаемая волатильность: {expected_volatility * 100:.2f}%\n"
        + f"Коэффициент Шарпа: {sharpe_ratio:.2f}"
    )

    fig, ax = plt.subplots(figsize=(15, 13))
    assets = weights
    labels = assets.keys()
    sizes = assets.values()

    ax.pie(sizes, labels=labels, autopct='%1.1f%%')
    ax.axis('equal')

    graph_path = f'portfolio_pie_chart{message.from_user.id}.png'
    plt.savefig(graph_path)

    with open(graph_path, 'rb') as photo:
        await message.answer_document(photo, caption=f"Ваш портфель по долям активов: {response}",
                                      reply_markup=main_inkb)

    os.remove(graph_path)
    # await bot.delete_message(message.from_user.id, message.message_id)
    await state.finish()


@ dp.callback_query_handler(lambda callback_query: callback_query.data == "check_portfolio")
async def check_portfolio(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = callback_query.from_user.id
    user_info = await user_db.get_user(user_id)

    await bot.edit_message_reply_markup(callback_query.from_user.id, callback_query.message.message_id, reply_markup=None)

    # Проверка наличия портфеля (пятый элемент в кортеже)
    if user_info and user_info[4]:
        portfolio = await user_db.get_portfolio(user_id)
        portfolio_str = portfolio[0]

        fig, ax = plt.subplots()
        assets = json.loads(portfolio_str)
        labels = assets.keys()
        sizes = assets.values()

        ax.pie(sizes, labels=labels, autopct='%1.1f%%')
        ax.axis('equal')

        graph_path = 'portfolio_pie_chart.png'
        plt.savefig(graph_path)

        with open(graph_path, 'rb') as photo:
            await bot.send_photo(callback_query.from_user.id, photo, caption="Ваш портфель по долям активов:")

        os.remove(graph_path)

        # Завершаем состояние после выдачи портфеля
        await state.finish()
    else:
        await callback_query.message.answer("Портфель не собран. Хотите собрать его сейчас?", reply_markup=yes_no_keyboard)


@ dp.callback_query_handler(state=None)
async def get_portfolio(callback_query: types.CallbackQuery, state: FSMContext):
    # await bot.edit_message_reply_markup(callback_query.from_user.id, callback_query.message.message_id, reply_markup=None)
    await bot.delete_message(callback_query.from_user.id, callback_query.message.message_id)
    if callback_query.data == "yes":
        await callback_query.message.answer("Выберите уровень риска:", reply_markup=risk_level_keyboard)
        await PortfolioStates.RISK_LEVEL.set()
    else:
        await callback_query.message.answer("Чем могу помочь сегодня?", reply_markup=main_inkb)
        await state.finish()


@ dp.callback_query_handler(Text(equals='Отмена', ignore_case=True), state="*")
async def cancel_handler(callback_query: types.CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    await state.finish()
    await bot.edit_message_reply_markup(callback_query.from_user.id, callback_query.message.message_id, reply_markup=None)
    await callback_query.message.reply("Действие отменено", reply_markup=main_inkb)
