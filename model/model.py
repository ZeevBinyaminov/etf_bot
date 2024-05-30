import pandas as pd
import numpy as np
import asyncio
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from model import moex
from database.database import moex_db

market = pd.read_excel('etf_market.xlsx')


def compute_returns(prices):
    """
    Compute daily returns from price data.

    :param prices: pd.DataFrame, asset prices with assets in columns and prices in rows
    :return: pd.DataFrame, asset returns
    """
    returns = mean_historical_return(prices)
    return returns


def compute_cov_matrix(prices):
    """
    Compute the covariance matrix for a given set of asset prices.

    :param prices: pd.DataFrame, asset prices with assets in columns and prices in rows
    :return: np.array, covariance matrix of the asset returns
    """

    cov_matrix = CovarianceShrinkage(prices).ledoit_wolf()
    return cov_matrix


def compute_liquidity_metrics(volumes, bid_prices, ask_prices):
    avg_trading_volume = volumes.mean()
    turnover_ratio = volumes.sum() / len(volumes)
    bid_ask_spread = (ask_prices - bid_prices).mean()
    time_to_sale = 1 / volumes.mean()  # Simplified metric

    liquidity_metrics = {
        'Average Trading Volume': avg_trading_volume,
        'Turnover Ratio': turnover_ratio,
        'Bid-Ask Spread': bid_ask_spread,
        'Time to Sale': time_to_sale
    }

    return liquidity_metrics


def adjust_cov_matrix_for_liquidity(cov_matrix, liquidity_metric, liquidity_level):
    if liquidity_metric == 'Average Trading Volume':
        adjustment = np.diag(1 / (liquidity_level + 1e-6)
                             )  # Avoid division by zero
    elif liquidity_metric == 'Turnover Ratio':
        adjustment = np.diag(liquidity_level)
    elif liquidity_metric == 'Bid-Ask Spread':
        adjustment = np.diag(liquidity_level * 0.01)
    elif liquidity_metric == 'Time to Sale':
        adjustment = np.diag(liquidity_level * 0.1)
    else:
        raise ValueError("Unknown liquidity metric")

    adjusted_cov_matrix = cov_matrix + adjustment
    return adjusted_cov_matrix


def compute_mu_black_litterman(mu_market, cov_matrix, P, Q, tau):
    """
    Computes the adjusted expected returns (mu) using the Black-Litterman model.

    :param mu_market: np.array, market expected returns
    :param cov_matrix: np.array, covariance matrix of returns
    :param P: np.array, matrix P for the Black-Litterman model
    :param Q: np.array, matrix Q for the Black-Litterman model
    :param tau: float, uncertainty parameter
    :return: np.array, adjusted expected returns (mu)
    """
    if P.shape[1] != cov_matrix.shape[0]:
        raise ValueError(
            "P matrix columns must match covariance matrix dimensions")

    if P.shape[0] != Q.shape[0]:
        raise ValueError("P matrix rows must match Q vector length")

    tau_cov_matrix = tau * cov_matrix
    omega = np.dot(np.dot(P, tau_cov_matrix), P.T)
    omega_inv = np.linalg.inv(omega)

    part1 = np.linalg.inv(np.linalg.inv(tau_cov_matrix) +
                          np.dot(np.dot(P.T, omega_inv), P))
    part2 = np.dot(np.linalg.inv(tau_cov_matrix), mu_market) + \
        np.dot(np.dot(P.T, omega_inv), Q)
    mu_adjusted = np.dot(part1, part2)

    return mu_adjusted


def compute_mu_market(cov_matrix, market_weights, delta):
    """
    Compute market-implied expected returns (mu_market) using reverse optimization.

    :param cov_matrix: np.array, covariance matrix of returns
    :param market_weights: np.array, market capitalization weights of the assets
    :param delta: float, risk aversion coefficient
    :return: np.array, market-implied expected returns
    """
    mu_market = delta * np.dot(cov_matrix, market_weights)
    return mu_market


def black_litterman_optimization(prices, liquidity_metric, optimization_goal='risk', target_return=None, target_risk=None):
    """
    Portfolio optimization using the Black-Litterman model with liquidity adjustments and optimization criteria.

    :param prices: pd.DataFrame, asset prices with assets in columns and prices in rows
    :param liquidity_metric: str, chosen liquidity metric
    :param optimization_goal: str, optimization criterion ('risk', 'return', 'liquidity')
    :param target_return: float, target return (used for return optimization)
    :param target_risk: float, target risk (used for return optimization)
    :return: np.array, optimal portfolio weights
    """
    tau = 0.05
    returns = compute_returns(prices)

    cov_matrix = compute_cov_matrix(prices)

    market['market_weights'] = market['СЧА, руб'] / market['СЧА, руб'].sum()
    market_weights = market['market_weights']

    mu_market = compute_mu_market(
        cov_matrix, market_weights=market_weights, delta=2.5)

    num_isins = len(prices.columns)

    # Assuming investor_views are equal weights across all assets
    investor_views = np.array([1/num_isins] * num_isins)
    P = np.identity(num_isins)
    Q = investor_views
    liquidity_levels = np.array([2] * num_isins)  # Example liquidity levels

    adjusted_cov_matrix = adjust_cov_matrix_for_liquidity(
        cov_matrix, liquidity_metric, liquidity_levels)

    mu_adjusted = compute_mu_black_litterman(
        mu_market, adjusted_cov_matrix, P, Q, tau)

    # Using PyPortfolioOpt for optimization
    ef = EfficientFrontier(mu_adjusted, adjusted_cov_matrix)

    if optimization_goal == 'risk':
        optimal_weights = ef.min_volatility()
    elif optimization_goal == 'return':
        if target_return is None:
            raise ValueError(
                "Target return must be specified for return optimization")
        ef.efficient_return(target_return=target_return)
        optimal_weights = ef.clean_weights()
    elif optimization_goal == 'liquidity':
        optimal_weights = ef.min_volatility()
    else:
        raise ValueError(
            "Unknown optimization goal: use 'risk', 'return', or 'liquidity'")

    cleaned_weights = ef.clean_weights()
    expected_return = ef.portfolio_performance(target_risk or 0.05)[0]
    expected_volatility = ef.portfolio_performance(target_risk or 0.05)[1]
    sharpe_ratio = ef.portfolio_performance(target_risk or 0.05)[2]

    # Создание словаря с метриками портфеля
    portfolio_stats = {
        "weights": dict(zip(market['Название'].to_list(), [round(x, 3) for x in cleaned_weights.values()])),
        "expected_return": round(expected_return, 3),
        "expected_volatility": round(expected_volatility, 3),
        "sharpe_ratio": round(sharpe_ratio, 3)
    }
    return portfolio_stats, [round(x, 3) for x in cleaned_weights.values()]


def check_and_clean_data(prices):
    """
    Check and clean the data for missing values.

    :param prices: pd.DataFrame, asset prices with assets in columns and prices in rows
    :return: pd.DataFrame, cleaned asset prices
    """
    if prices.isnull().values.any():
        prices = prices.ffill()
    return prices


async def model(optimization_goal='risk', target_return=None, target_risk=0.04, liquidity_metric='Average Trading Volume'):
    open_prices, close_prices, volumes = await moex_db.get_cached_moex_data()

    close_prices, open_prices, volumes = [check_and_clean_data(data)
                                          for data in (close_prices, open_prices, volumes)]
    if optimization_goal == 'risk':
        portfolio_stats, mu_adjusted = black_litterman_optimization(
            close_prices, liquidity_metric=liquidity_metric, optimization_goal='risk', target_risk=target_risk)
    elif optimization_goal == 'return':
        print('Тут')
        portfolio_stats, mu_adjusted = black_litterman_optimization(
            close_prices, liquidity_metric=liquidity_metric, optimization_goal='return', target_return=target_return)
    elif optimization_goal == 'liquidity':
        portfolio_stats, mu_adjusted = black_litterman_optimization(
            close_prices, liquidity_metric=liquidity_metric, optimization_goal='liquidity')

    return portfolio_stats, mu_adjusted


#    Average Trading Volume': avg_trading_volume,
#    Turnover Ratio': turnover_ratio,
#    Bid-Ask Spread': bid_ask_spread,
#    Time to Sale': time_to_sale
