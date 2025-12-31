# Product Guidelines

## Documentation Style
- **Technical and Precise:** All documentation, including READMEs, design documents, and code comments, should be written with high technical accuracy. Avoid ambiguity. Use standard industry terminology (e.g., "SCD Type 2", "Z-Ordering", "Delta Lake").
- **Conciseness:** Value brevity. Explain the "why" and "how" clearly without unnecessary fluff.
- **Example-Driven:** Where complex logic is involved (e.g., bitwise flag operations, fixed-point math), provide concrete examples.

## Design Principles
- **Correctness First:** Data integrity is non-negotiable. The ETL process must be deterministic. If a trade-off exists between a slightly faster pipeline and ensuring point-in-time correctness, always choose correctness.
- **Reproducibility:** Any transformation must be reproducible from the raw Bronze data. Use explicit versioning for code and metadata.
- **Explicit over Implicit:** Do not rely on "magic" default behaviors. Configuration should be explicit. Timestamps should clearly state their timezone (e.g., `ts_local_us`).

## Code Standards
- **Type Safety:** Use strict typing (Python type hints) throughout the codebase to catch errors early.
- **Error Handling:** Fail fast. If an anomaly is detected in the data stream (e.g., a crossed book), the pipeline should flag it immediately rather than silently propagating bad data.
