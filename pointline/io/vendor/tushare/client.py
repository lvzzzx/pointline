"""Tushare API client for Chinese stock data."""

from __future__ import annotations

import os
from typing import Literal

import polars as pl


class TushareClient:
    """Client for Tushare Pro API."""

    def __init__(self, token: str | None = None):
        """
        Initialize Tushare client.

        Args:
            token: Tushare API token (or use TUSHARE_TOKEN env var)

        Raises:
            ValueError: If no token provided
            ImportError: If tushare package not installed
        """
        try:
            import tushare as ts
        except ImportError:
            raise ImportError("tushare package required. Install with: pip install tushare")

        token = token or os.getenv("TUSHARE_TOKEN")
        if not token:
            raise ValueError(
                "Tushare token required. Set TUSHARE_TOKEN env var or pass token parameter.\n"
                "Get token from: https://tushare.pro/user/token"
            )

        ts.set_token(token)
        self.pro = ts.pro_api()
        self._ts = ts

    def get_stock_basic(
        self,
        exchange: str | None = None,
        list_status: str | None = "L",
    ) -> pl.DataFrame:
        """
        Fetch basic stock information from Tushare.

        Args:
            exchange: Filter by exchange ("SSE", "SZSE", "BSE", or None for all)
            list_status:
                "L" = Listed (default)
                "D" = Delisted
                "P" = Suspended
                None = All statuses

        Returns:
            Polars DataFrame with columns:
                ts_code, symbol, name, area, industry, fullname, enname,
                market, exchange, list_status, list_date, delist_date, is_hs

        Example:
            >>> client = TushareClient()
            >>> df = client.get_stock_basic(exchange="SZSE", list_status="L")
            >>> print(df.head())
        """
        df_pandas = self.pro.stock_basic(
            exchange=exchange or "",
            list_status=list_status or "",
            fields="ts_code,symbol,name,area,industry,fullname,enname,"
            "market,exchange,list_status,list_date,delist_date,is_hs",
        )

        # Convert to Polars
        df = pl.from_pandas(df_pandas)

        return df

    def get_szse_stocks(self, include_delisted: bool = False) -> pl.DataFrame:
        """
        Get all Shenzhen Stock Exchange (SZSE) stocks.

        Args:
            include_delisted: Include delisted stocks

        Returns:
            DataFrame with SZSE stocks only

        Example:
            >>> client = TushareClient()
            >>> df = client.get_szse_stocks()
            >>> print(f"Found {len(df)} SZSE stocks")
        """
        if include_delisted:
            df = self.get_stock_basic(exchange="SZSE", list_status=None)
        else:
            df = self.get_stock_basic(exchange="SZSE", list_status="L")

        return df

    def get_sse_stocks(self, include_delisted: bool = False) -> pl.DataFrame:
        """
        Get all Shanghai Stock Exchange (SSE) stocks.

        Args:
            include_delisted: Include delisted stocks

        Returns:
            DataFrame with SSE stocks only

        Example:
            >>> client = TushareClient()
            >>> df = client.get_sse_stocks()
            >>> print(f"Found {len(df)} SSE stocks")
        """
        if include_delisted:
            df = self.get_stock_basic(exchange="SSE", list_status=None)
        else:
            df = self.get_stock_basic(exchange="SSE", list_status="L")

        return df

    def get_all_stocks(
        self,
        exchanges: list[Literal["SZSE", "SSE", "BSE"]] | None = None,
        include_delisted: bool = False,
    ) -> pl.DataFrame:
        """
        Get stocks from multiple exchanges.

        Args:
            exchanges: List of exchanges to fetch (default: ["SZSE", "SSE"])
            include_delisted: Include delisted stocks

        Returns:
            Combined DataFrame with all requested stocks

        Example:
            >>> client = TushareClient()
            >>> df = client.get_all_stocks(exchanges=["SZSE", "SSE"])
            >>> print(df.groupby("exchange").count())
        """
        if exchanges is None:
            exchanges = ["SZSE", "SSE"]

        dfs = []
        for exchange in exchanges:
            df = self.get_stock_basic(
                exchange=exchange, list_status=None if include_delisted else "L"
            )
            dfs.append(df)

        return pl.concat(dfs) if dfs else pl.DataFrame()
