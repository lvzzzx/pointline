#!/usr/bin/env python
"""Example: Co-Trading Networks for Chinese A-Shares

This script demonstrates building co-trading networks using Pointline v2
to model dynamic interdependency structures between Chinese stocks.

Reference: Lu et al. (2023) arXiv:2302.09382

Run: python examples/cotrading_networks_cn_example.py

Author: Quant Research
Date: 2026-02-13
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from pointline.research import discovery, query
from pointline.research.cn_trading_phases import TradingPhase, filter_by_phase

if TYPE_CHECKING:
    import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

SILVER_ROOT = Path(os.environ.get("POINTLINE_SILVER_ROOT", "~/data/lake/silver")).expanduser()
EXCHANGE = "szse"  # or "sse"
TRADING_DATE = "2024-09-30"
DELTA_T_US = 1_000  # 1ms window for co-trading
TOP_K_NEIGHBORS = 10


def get_day_bounds(trading_date: str) -> tuple[int, int]:
    """Convert trading date to microsecond timestamps."""
    date_dt = datetime.strptime(trading_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_start_us = int(date_dt.timestamp() * 1_000_000)
    day_end_us = day_start_us + 86_400_000_000  # +24 hours
    return day_start_us, day_end_us


def load_universe_data(
    silver_root: Path,
    universe: list[str],
    exchange: str,
    trading_date: str,
    exclude_auctions: bool = True,
) -> dict[str, pl.DataFrame]:
    """Load tick data for universe of stocks from cn_tick_events."""
    data: dict[str, pl.DataFrame] = {}
    day_start_us, day_end_us = get_day_bounds(trading_date)

    for symbol in universe:
        ticks = query.load_events(
            silver_root=silver_root,
            table="cn_tick_events",
            exchange=exchange,
            symbol=symbol,
            start=day_start_us,
            end=day_end_us,
            columns=[
                "ts_event_us",
                "ts_local_us",
                "price",
                "qty",
                "aggressor_side",
                "bid_order_ref",
                "ask_order_ref",
            ],
        )

        if ticks.is_empty():
            print(f"  No data for {symbol}")
            continue

        # Filter to continuous trading only (exclude auctions)
        if exclude_auctions:
            ticks = filter_by_phase(
                ticks,
                exchange=exchange,
                phases=[TradingPhase.MORNING, TradingPhase.AFTERNOON],
                ts_col="ts_event_us",
            )

        # Keep only trades with valid aggressor_side (actual fills)
        ticks = ticks.filter(pl.col("aggressor_side").is_not_null())

        if not ticks.is_empty():
            data[symbol] = ticks.sort("ts_event_us")
            print(f"  Loaded {symbol}: {len(ticks)} trades")

    return data


def compute_cotrading_similarity(
    data: dict[str, pl.DataFrame],
    delta_t_us: int = 1_000,
    volume_weighted: bool = True,
) -> pd.DataFrame:
    """Compute pairwise co-trading similarity matrix."""
    import pandas as pd

    symbols = list(data.keys())
    n = len(symbols)

    if n == 0:
        raise ValueError("No data to compute similarity")

    # Initialize similarity matrix
    similarity = np.zeros((n, n))

    # Pre-compute total volumes
    total_volumes = {}
    for symbol in symbols:
        total_volumes[symbol] = data[symbol]["qty"].sum()

    print(f"\nComputing {n}x{n} similarity matrix...")

    # Compute pairwise co-trading
    for i, sym_i in enumerate(symbols):
        df_i = data[sym_i].select(["ts_event_us", "qty"]).to_pandas()

        for j, sym_j in enumerate(symbols[i + 1 :], start=i + 1):
            df_j = data[sym_j].select(["ts_event_us", "qty"]).to_pandas()

            # Find concurrent trades using merge_asof
            concurrent = pd.merge_asof(
                df_i,
                df_j,
                on="ts_event_us",
                direction="nearest",
                tolerance=delta_t_us,
            )

            # Remove NaN (no match within delta_t)
            concurrent = concurrent.dropna()

            if volume_weighted:
                # Weight by minimum quantity
                co_trading_volume = concurrent[["qty_x", "qty_y"]].min(axis=1).sum()
            else:
                # Count matches
                co_trading_volume = len(concurrent)

            # Normalized similarity
            norm = np.sqrt(total_volumes[sym_i] * total_volumes[sym_j])
            similarity[i, j] = co_trading_volume / norm if norm > 0 else 0
            similarity[j, i] = similarity[i, j]

        if (i + 1) % 5 == 0 or i == n - 1:
            print(f"  Progress: {i + 1}/{n} symbols processed")

    return pd.DataFrame(similarity, index=symbols, columns=symbols)


def build_cotrading_network(
    similarity_matrix: pd.DataFrame,
    threshold: float | None = None,
    k_nearest: int = 10,
):
    """Build networkx graph from similarity matrix."""
    try:
        import networkx as nx
    except ImportError:
        print("NetworkX not installed. Install with: pip install networkx")
        raise

    symbols = similarity_matrix.index.tolist()

    # Adaptive threshold (mean + std)
    if threshold is None:
        sim_values = similarity_matrix.values
        mask = np.triu(np.ones_like(sim_values, dtype=bool), k=1)
        upper_tri = sim_values[mask]
        threshold = float(upper_tri.mean() + upper_tri.std())
        print(f"\nAdaptive threshold: {threshold:.4f}")

    # Create graph
    G = nx.Graph()
    G.add_nodes_from(symbols)

    # Add edges above threshold OR k-nearest
    edge_count = 0
    for i, sym_i in enumerate(symbols):
        # Get top-k neighbors
        neighbors = similarity_matrix.iloc[i].nlargest(k_nearest + 1)
        neighbors = neighbors[neighbors.index != sym_i]  # Remove self

        for sym_j, sim in neighbors.items():
            if sim >= threshold and not G.has_edge(sym_i, sym_j):
                G.add_edge(sym_i, sym_j, weight=float(sim))
                edge_count += 1

    print(f"Network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G


def spectral_cluster_stocks(
    similarity_matrix: pd.DataFrame,
    n_clusters: int = 5,
) -> pd.DataFrame:
    """Apply spectral clustering to discover data-driven sectors."""
    import pandas as pd
    from sklearn.cluster import SpectralClustering

    affinity_matrix = similarity_matrix.values.copy()
    affinity_matrix = (affinity_matrix + affinity_matrix.T) / 2  # Ensure symmetric

    clustering = SpectralClustering(
        n_clusters=n_clusters,
        affinity="precomputed",
        assign_labels="kmeans",
        random_state=42,
    )

    labels = clustering.fit_predict(affinity_matrix)

    return pd.DataFrame(
        {
            "symbol": similarity_matrix.index,
            "cluster": labels,
            "cluster_name": [f"CT_{n_clusters}_{label}" for label in labels],
        }
    )


def analyze_network_centrality(graph):
    """Compute centrality measures for network analysis."""
    import networkx as nx
    import pandas as pd

    nodes = list(graph.nodes())
    centrality = pd.DataFrame(
        {
            "symbol": nodes,
            "degree": [graph.degree(n) for n in nodes],
            "weighted_degree": [graph.degree(n, weight="weight") for n in nodes],
        }
    )

    if len(nodes) > 0 and len(graph.edges()) > 0:
        try:
            bc = nx.betweenness_centrality(graph, weight="weight")
            centrality["betweenness"] = [bc.get(n, 0) for n in nodes]
        except Exception:
            centrality["betweenness"] = 0.0
    else:
        centrality["betweenness"] = 0.0

    return centrality.sort_values("weighted_degree", ascending=False)


def main():
    """Main example execution."""
    print("=" * 60)
    print("Co-Trading Networks for Chinese A-Shares")
    print("=" * 60)
    print(f"Exchange: {EXCHANGE}")
    print(f"Date: {TRADING_DATE}")
    print(f"Silver root: {SILVER_ROOT}")
    print()

    # Verify silver root exists
    if not SILVER_ROOT.exists():
        print(f"ERROR: Silver root not found: {SILVER_ROOT}")
        print("Set POINTLINE_SILVER_ROOT environment variable or update SILVER_ROOT")
        return

    # Step 1: Discover symbols
    print("Step 1: Discovering symbols...")
    symbols_df = discovery.discover_symbols(
        silver_root=SILVER_ROOT,
        exchange=EXCHANGE,
        limit=20,  # Small universe for demo
    )

    if symbols_df.is_empty():
        print("No symbols found. Check data availability.")
        return

    universe = symbols_df["canonical_symbol"].to_list()
    print(f"Selected {len(universe)} symbols")
    print()

    # Step 2: Load tick data
    print("Step 2: Loading tick data...")
    data = load_universe_data(
        silver_root=SILVER_ROOT,
        universe=universe,
        exchange=EXCHANGE,
        trading_date=TRADING_DATE,
        exclude_auctions=True,
    )

    if len(data) < 3:
        print("Insufficient data for network analysis (need at least 3 symbols)")
        return

    print(f"Loaded data for {len(data)} symbols")
    print()

    # Step 3: Compute co-trading similarity
    print("Step 3: Computing co-trading similarity...")
    similarity = compute_cotrading_similarity(
        data,
        delta_t_us=DELTA_T_US,
        volume_weighted=True,
    )

    print(f"\nSimilarity matrix shape: {similarity.shape}")
    print(
        f"Mean similarity: {similarity.values[np.triu_indices_from(similarity.values, k=1)].mean():.4f}"
    )
    print()

    # Step 4: Build network
    print("Step 4: Building co-trading network...")
    G = build_cotrading_network(
        similarity,
        threshold=None,  # Use adaptive
        k_nearest=TOP_K_NEIGHBORS,
    )
    print()

    # Step 5: Spectral clustering
    print("Step 5: Spectral clustering...")
    n_clusters = min(5, len(data))
    clusters = spectral_cluster_stocks(similarity, n_clusters=n_clusters)

    print(f"\nDiscovered {n_clusters} clusters:")
    for c in range(n_clusters):
        cluster_symbols = clusters[clusters["cluster"] == c]["symbol"].tolist()
        print(
            f"  Cluster {c}: {', '.join(cluster_symbols[:5])}{'...' if len(cluster_symbols) > 5 else ''}"
        )
    print()

    # Step 6: Network centrality
    print("Step 6: Network centrality analysis...")
    centrality = analyze_network_centrality(G)

    print("\nTop 5 most connected stocks:")
    for _, row in centrality.head(5).iterrows():
        print(f"  {row['symbol']}: degree={row['degree']}, weighted={row['weighted_degree']:.2f}")
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Universe size: {len(data)} stocks")
    print(f"Network edges: {G.number_of_edges()}")
    print(f"Clusters: {n_clusters}")
    print(f"Avg cluster size: {len(clusters) / n_clusters:.1f}")
    print()
    print("Next steps:")
    print("- Validate: correlation(similarity, future_return_corr)")
    print("- Extend: multi-day cluster stability analysis")
    print("- Apply: network-shrunk covariance for portfolio optimization")


if __name__ == "__main__":
    main()
