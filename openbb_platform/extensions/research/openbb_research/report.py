"""Self-contained HTML with escaped metadata and inline, numerical SVG plots."""

import html
import json

from openbb_alpha.store import Store

from openbb_research.workflow import result


def escaped_json(value):
    return html.escape(json.dumps(value, sort_keys=True, indent=2, allow_nan=False))


def plot(points):
    values = [(i, p["spearman_ic"]) for i, p in enumerate(points) if p["spearman_ic"] is not None]
    if not values:
        return "<p>No defined IC observations.</p>"
    width = max(1, len(points) - 1)
    coordinates = " ".join(f"{10 + i / width * 580:.2f},{85 - v * 70:.2f}" for i, v in values)
    return (
        '<svg viewBox="0 0 600 170" role="img" aria-label="Daily Spearman IC, minus one to one">'
        '<path d="M10 85H590" stroke="#b8c1cc"/>'
        f'<polyline points="{coordinates}" fill="none" stroke="#166b8c" stroke-width="1.3"/>'
        "</svg>"
    )


def report(manifest_id: str):
    store = Store()
    manifest = result(manifest_id)
    evaluation = (
        store.get_json(manifest["outputs"]["evaluation"], "evaluation")
        if "evaluation" in manifest["outputs"]
        else None
    )
    sections = [
        "<!doctype html><html lang='en'><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>OpenBB Alpha Research</title><style>body{font:16px system-ui;max-width:1100px;"
        "margin:40px auto;padding:0 20px;color:#183146;background:#fafbfc}"
        "pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf2f6;padding:16px}"
        "table{border-collapse:collapse;width:100%}td,th{padding:8px;border-bottom:1px solid #ccd}"
        "svg{max-width:700px;width:100%}section{margin:32px 0}</style><body>",
        "<h1>OpenBB Alpha Research</h1><p>Lab research. "
        "Synthetic fixtures are not investment evidence. "
        "IC and quantile spreads are diagnostics, not strategy NAV.</p>",
        f"<p>Manifest: <code>{html.escape(manifest_id)}</code></p>",
        f"<p>Status: <strong>{html.escape(manifest['status'])}</strong></p>",
        "<section><h2>Configuration, identities and provenance</h2><pre>"
        + escaped_json(manifest)
        + "</pre></section>",
    ]
    if evaluation:
        sections.append(
            "<section><h2>Coverage and loss accounting</h2><pre>"
            + escaped_json(evaluation["coverage"])
            + "</pre></section>"
        )
        for metric in evaluation["metrics"]:
            sections.append(
                f"<section><h2>{html.escape(metric['factor_id'])}: "
                f"{int(metric['horizon_sessions'])} sessions</h2>"
            )
            sections.append(plot(metric["daily_ic"]))
            sections.append(
                "<pre>"
                + escaped_json(
                    {
                        k: v
                        for k, v in metric.items()
                        if k not in ("daily_ic", "quantile_turnover", "rank_persistence")
                    }
                )
                + "</pre>"
            )
            sections.append(
                "<details><summary>Daily metrics, persistence and turnover</summary><pre>"
                + escaped_json(metric)
                + "</pre></details></section>"
            )
        sections.append(
            "<section><h2>Method and limitations</h2><pre>"
            + escaped_json(
                {"configuration": evaluation["configuration"], "warnings": evaluation["warnings"]}
            )
            + "</pre></section>"
        )
    sections.append("</body></html>")
    content = "\n".join(sections)
    key = store.put(content.encode())
    return {
        "artifact_id": key,
        "media_type": "text/html",
        "manifest_id": manifest_id,
        "html": content,
    }
