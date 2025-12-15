import re
from pathlib import Path


def test_dialog_modal_hidden_until_open() -> None:
    css = Path("static/css/main.css").read_text(encoding="utf-8")

    modal_block = re.search(r"dialog\.modal\s*\{[^}]*\}", css, re.DOTALL)
    assert modal_block, "Expected a `dialog.modal { ... }` block in static/css/main.css"
    assert "display: none;" in modal_block.group(0)

    open_block = re.search(r"dialog\.modal\[open\]\s*\{[^}]*\}", css, re.DOTALL)
    assert open_block, (
        "Expected a `dialog.modal[open] { ... }` block in static/css/main.css"
    )
    assert "display: flex;" in open_block.group(0)


def test_filter_bar_pills_do_not_wrap() -> None:
    css = Path("static/css/main.css").read_text(encoding="utf-8")

    rule = re.search(r"\.filter-bar\s+\.pill\s*\{[^}]*\}", css, re.DOTALL)
    assert rule, "Expected a `.filter-bar .pill { ... }` rule in static/css/main.css"
    assert "white-space: nowrap;" in rule.group(0)
