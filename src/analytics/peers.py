"""Peer model v1 -- ten most structurally similar communes, per
docs/features/peer_model.md and ADR 0015 (docs/decisions/0015-peer-model-v1.md).

PURE MODULE. numpy + pandas only, no I/O, no scikit-learn (the maintainer's
2026-09-26 decision, ADR 0015 Decision 2). The caller (scripts/export_peer_model.py)
reads the committed CSVs, builds the raw feature values, and hands them here;
everything here is deterministic given those inputs.

Method, exactly as the spec defines it:

- Standardisation: z = (x - mean) / std, ddof=0 (population std, divide by N)
  -- numerically identical to sklearn.preprocessing.StandardScaler, proven by
  a unit test rather than merely asserted (ADR 0015 Decision 2, Risk 2).
- Distance: plain Euclidean on the standardised matrix, equal weights.
- k = 10, ties broken by NIS ascending, a commune is never its own peer.
- Missing/non-finite/non-positive-before-log values REFUSE the whole run
  (CLAUDE.md rule 13) -- no imputation in v1.
- PCA is a second, optional variant (off by default): SVD, kept components
  reach >= 90% cumulative explained variance, deterministic sign convention.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

#: Bumped on any change to the variable list, a variable's period, or the
#: method (standardisation, distance, k, PCA threshold) -- ADR 0015
#: "Versioning". Stays "1.0.0-rc.1" until the maintainer's 15-commune manual
#: review and a red-team audit find nothing that changes either.
PEER_MODEL_VERSION = "1.0.0-rc.1"

#: Every commune gets exactly this many peers per list (subject to pool size,
#: which is >= 18 everywhere today -- ADR 0015 Decision 1).
K = 10

#: Fewest PCA components whose cumulative explained variance reaches this
#: fraction. Off by default; only used when the caller opts into the PCA
#: variant.
PCA_MIN_VARIANCE = 0.90


class PeerModelError(ValueError):
    """The inputs do not support building the peer model as specified.

    Raised rather than silently coercing/dropping rows (CLAUDE.md rule 13):
    missing values, non-finite values, non-positive values before a log
    transform, and zero-variance columns are all schema-level surprises the
    spec requires the run to refuse on, not paper over.
    """


# ── The variable list (ADR 0015 Decision 3 -- all eleven kept, maintainer's
#    2026-09-26 approval) ─────────────────────────────────────────────────────
#
# Each entry: id, the raw indicator(s)/ratio it is built from (documentation
# only -- the caller does the actual reading and ratio computation; this
# module only standardises and measures distance), the period(s) the spec
# names (NOT "latest" computed at runtime -- every period below is the exact
# one docs/features/peer_model.md's variable table states), and the
# transform ("none" or "log", natural log per the spec).
#
# The caller (export_peer_model.py) builds one float column per id, named by
# `id`, and passes the resulting DataFrame to build_feature_matrix.


class Variable:
    __slots__ = ("id", "period", "transform", "description")

    def __init__(self, id: str, period: str, transform: str, description: str):
        if transform not in ("none", "log"):
            raise PeerModelError(f"variable {id!r}: unknown transform {transform!r}")
        self.id = id
        self.period = period
        self.transform = transform
        self.description = description

    def __repr__(self) -> str:  # pragma: no cover - debugging aid only
        return f"Variable({self.id!r}, period={self.period!r}, transform={self.transform!r})"


#: All eleven variables the maintainer approved 2026-09-26. Order matches the
#: spec's table (docs/features/peer_model.md, "The variable list").
VARIABLES: tuple[Variable, ...] = (
    Variable(
        "population",
        period="2026",
        transform="log",
        description="POPULATION_BY_COMMUNE 2026",
    ),
    Variable(
        "population_density",
        period="2026",
        transform="log",
        description="POPULATION_BY_COMMUNE 2026 / area_km2 (config/geography/commune_area_km2.csv)",
    ),
    Variable(
        "share_65_plus",
        period="2026",
        transform="none",
        description="POPULATION_AGE_65_PLUS / POPULATION_BY_COMMUNE, 2026",
    ),
    Variable(
        "share_0_14",
        period="2026",
        transform="none",
        description="POPULATION_AGE_0_14 / POPULATION_BY_COMMUNE, 2026",
    ),
    Variable(
        "population_change_5y",
        period="2026",
        transform="none",
        description="POPULATION_CHANGE_5Y (derived), 2026",
    ),
    Variable(
        "avg_net_taxable_income",
        period="2023",
        transform="none",
        description="AVG_NET_TAXABLE_INCOME (derived), 2023",
    ),
    Variable(
        "unemployment_rate_insured",
        period="2026",
        transform="none",
        description="UNEMPLOYMENT_RATE_INSURED, 2026 (provisional)",
    ),
    Variable(
        "share_foreign_nationals",
        period="2021",
        transform="none",
        description="SHARE_FOREIGN_NATIONALS (derived), 2021 census",
    ),
    Variable(
        "average_household_size",
        period="2021",
        transform="none",
        description="AVERAGE_HOUSEHOLD_SIZE (derived), 2021 census",
    ),
    Variable(
        "enterprise_density",
        period="2023-Q4",
        transform="log",
        description="LOCAL_UNITS_BY_COMMUNE 2023-Q4 per 1,000 residents (POPULATION_BY_COMMUNE 2023)",
    ),
    Variable(
        "property_tax_base_per_resident",
        period="2026",
        transform="log",
        description="MUN_CADASTRAL_INCOME_TOTAL 2026 / POPULATION_BY_COMMUNE 2026",
    ),
)

VARIABLE_IDS: tuple[str, ...] = tuple(v.id for v in VARIABLES)
VARIABLES_BY_ID: dict[str, Variable] = {v.id: v for v in VARIABLES}


def build_feature_matrix(
    values: pd.DataFrame, variables: tuple[Variable, ...] = VARIABLES
) -> pd.DataFrame:
    """565 (or fewer, in a test fixture) rows x len(variables) columns.

    `values` is indexed by NIS and has one column per variable id, holding
    the RAW ratio/count already computed in Python by the caller (this
    function does no ratio arithmetic of its own -- it applies only the
    `log` transform the spec names). Returns a new DataFrame, same index
    (sorted), `log`-transformed where the variable says so.

    Refuses (raises PeerModelError) on:
      - any variable id in `variables` missing from `values.columns`;
      - any missing (NaN) value, for any row, for any variable;
      - any non-finite value (inf/-inf);
      - any non-positive value in a column the spec logs (log(0) or
        log(negative) is undefined, and the spec states this must never
        happen for real data -- CLAUDE.md rule 13, fail loudly rather than
        floor or drop).

    Every refusal message lists every offending (nis, variable) pair, not
    just the first, since a builder or reviewer fixing the input wants the
    whole list at once.
    """
    missing_cols = [v.id for v in variables if v.id not in values.columns]
    if missing_cols:
        raise PeerModelError(f"feature matrix missing columns: {missing_cols}")

    ordered = values.loc[:, [v.id for v in variables]].sort_index()

    offenders: list[tuple[str, str, str]] = []  # (nis, variable, reason)
    for variable in variables:
        col = ordered[variable.id]
        is_na = col.isna()
        is_inf = np.isinf(col.to_numpy(dtype="float64", na_value=0.0)) & ~is_na
        for nis in col.index[is_na]:
            offenders.append((str(nis), variable.id, "missing"))
        for nis in col.index[is_inf]:
            offenders.append((str(nis), variable.id, "non-finite"))
        if variable.transform == "log":
            non_positive = (~is_na) & (~is_inf) & (col <= 0)
            for nis in col.index[non_positive]:
                offenders.append((str(nis), variable.id, "non-positive before log"))

    if offenders:
        detail = "; ".join(f"{nis}/{var}: {reason}" for nis, var, reason in offenders)
        raise PeerModelError(
            f"feature matrix refused -- {len(offenders)} offending (nis, variable) cell(s): "
            f"{detail}"
        )

    result = ordered.copy()
    for variable in variables:
        if variable.transform == "log":
            result[variable.id] = np.log(result[variable.id].to_numpy(dtype="float64"))

    return result


def standardise(X: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """z = (x - mean) / std, ddof=0 -- exactly sklearn's StandardScaler.

    Returns (Z, means, stds), all indexed/columned the same as X (means/stds
    indexed by column name). Refuses if any column has std == 0 (every
    commune identical on that variable): dividing by zero would silently
    propagate inf/nan into every distance (CLAUDE.md rule 13).
    """
    means = X.mean(axis=0, skipna=False)
    stds = X.std(axis=0, ddof=0, skipna=False)

    zero_std = stds[stds == 0]
    if len(zero_std) > 0:
        raise PeerModelError(f"zero-variance column(s), cannot standardise: {list(zero_std.index)}")

    Z = (X - means) / stds
    return Z, means, stds


def pairwise_distances(Z: pd.DataFrame) -> pd.DataFrame:
    """Plain Euclidean distance matrix, indexed and columned by Z's index
    (NIS). Symmetric, zero diagonal."""
    matrix = Z.to_numpy(dtype="float64")
    # (a-b)^2 summed over columns = |a|^2 + |b|^2 - 2 a.b -- computed this way
    # (rather than a naive O(n^2 * k) python loop) purely for speed; the
    # result is exact Euclidean distance up to floating-point rounding at
    # the ~1e-12 level, which the tests' hand-computed values do not depend
    # on beyond 4-5 decimal places.
    sq_norms = np.sum(matrix**2, axis=1)
    dots = matrix @ matrix.T
    sq_dist = sq_norms[:, None] + sq_norms[None, :] - 2 * dots
    # Clamp tiny negative values from floating-point cancellation (a
    # commune's distance to itself can compute as -1e-16 instead of 0).
    sq_dist = np.maximum(sq_dist, 0.0)
    dist = np.sqrt(sq_dist)
    return pd.DataFrame(dist, index=Z.index, columns=Z.index)


def nearest(
    D: pd.DataFrame,
    nis_index: list[str] | pd.Index,
    k: int = K,
    pool: dict[str, list[str]] | None = None,
) -> dict[str, list[tuple[str, float]]]:
    """For every NIS in `nis_index`, its k nearest peers by distance,
    excluding itself, restricted to `pool[nis]` if given (region list) or
    every other NIS in `nis_index` if not (national list).

    Ties broken by NIS ascending. Returns exactly k peers unless the pool
    (minus self) has fewer than k members, in which case it refuses --
    "return fewer than k silently" is explicitly not a case the spec wants
    handled by an unstated convention; no pool today is smaller than 18, so
    this never binds in practice, and a fixture that deliberately shrinks a
    pool below k must expect this refusal, not a short list.
    """
    result: dict[str, list[tuple[str, float]]] = {}
    for nis in nis_index:
        candidates = [n for n in (pool[nis] if pool is not None else nis_index) if n != nis]
        if len(candidates) < k:
            raise PeerModelError(
                f"commune {nis}: pool has only {len(candidates)} candidates, fewer than k={k}"
            )
        # Sort by (distance, nis) so ties break by NIS ascending regardless
        # of input order or floating-point sort instability.
        ordered = sorted(candidates, key=lambda c: (D.loc[nis, c], c))
        result[nis] = [(c, float(D.loc[nis, c])) for c in ordered[:k]]
    return result


def similarity_score(distance: float, d_max_national: float) -> float:
    """Display-only 0-100 score: 100 * (1 - d / d_max_national).

    d_max_national is the LARGEST of the ten national distances actually
    returned for the commune the peer belongs to -- never a global constant
    (docs/features/peer_model.md, "Similarity score for display"). Not
    itself a measured similarity; the underlying figure is always the
    standardised Euclidean distance.
    """
    if d_max_national <= 0:
        raise PeerModelError("d_max_national must be positive to compute a similarity score")
    return 100.0 * (1.0 - distance / d_max_national)


def pca_reduce(
    Z: pd.DataFrame, min_variance: float = PCA_MIN_VARIANCE
) -> tuple[pd.DataFrame, np.ndarray]:
    """SVD on the standardised matrix; keep the fewest components whose
    cumulative explained variance ratio is >= min_variance.

    Deterministic sign convention: for each kept component, if the entry of
    largest absolute value in its loading vector is negative, flip the sign
    of both the loading and the corresponding scores column. Without this,
    numpy.linalg.svd's sign is only unique up to +/-1 per component and is
    not guaranteed stable across numerically-equivalent inputs.

    Returns (scores, explained_variance_ratio) where `scores` is a
    DataFrame indexed like Z with one column per kept component
    ("pc1", "pc2", ...), and explained_variance_ratio is the full
    (not just kept) array of every component's ratio, for reporting.
    """
    matrix = Z.to_numpy(dtype="float64")
    n = matrix.shape[0]
    # Economy SVD: matrix = U @ diag(S) @ Vt
    U, S, Vt = np.linalg.svd(matrix, full_matrices=False)

    variance = S**2 / (n - 1) if n > 1 else S**2
    total_variance = variance.sum()
    if total_variance <= 0:
        raise PeerModelError("PCA refused: zero total variance in standardised matrix")
    explained_ratio = variance / total_variance

    cumulative = np.cumsum(explained_ratio)
    n_components = int(np.searchsorted(cumulative, min_variance) + 1)
    n_components = min(n_components, matrix.shape[1])

    loadings = Vt[:n_components]  # shape (n_components, n_features)
    scores = U[:, :n_components] * S[:n_components]  # shape (n, n_components)

    for i in range(n_components):
        largest_idx = np.argmax(np.abs(loadings[i]))
        if loadings[i, largest_idx] < 0:
            loadings[i] *= -1
            scores[:, i] *= -1

    columns = [f"pc{i + 1}" for i in range(n_components)]
    scores_df = pd.DataFrame(scores, index=Z.index, columns=columns)
    return scores_df, explained_ratio
