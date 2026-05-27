"""Reporting exports."""

__all__ = [
    "CandidateExportResult",
    "PaperPerformance",
    "export_latest_candidates",
    "load_paper_performance",
]


def __getattr__(name: str):
    if name in {"CandidateExportResult", "export_latest_candidates"}:
        from tw_quant.reporting.export import CandidateExportResult, export_latest_candidates

        return {
            "CandidateExportResult": CandidateExportResult,
            "export_latest_candidates": export_latest_candidates,
        }[name]
    if name in {"PaperPerformance", "load_paper_performance"}:
        from tw_quant.reporting.performance import PaperPerformance, load_paper_performance

        return {
            "PaperPerformance": PaperPerformance,
            "load_paper_performance": load_paper_performance,
        }[name]
    raise AttributeError(name)
