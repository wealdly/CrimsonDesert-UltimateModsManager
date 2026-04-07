"""Validation results dialog — shows mod compatibility check results."""
import logging

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QVBoxLayout, QWidget,
)

from cdumm.engine.mod_validator import ValidationIssue

logger = logging.getLogger(__name__)

SEVERITY_COLORS = {
    "error": "#FF4444",
    "warning": "#FFAA00",
}

SEVERITY_LABELS = {
    "error": "ERROR",
    "warning": "WARNING",
}


class ValidationDialog(QDialog):
    """Shows mod compatibility validation results grouped by mod."""

    def __init__(self, issues: list[ValidationIssue], parent=None) -> None:
        super().__init__(parent)
        self._issues = issues
        self._should_disable = False
        self.setWindowTitle("Mod Compatibility Check")
        self.setMinimumSize(720, 500)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        errors = [i for i in self._issues if i.severity == "error"]
        warnings = [i for i in self._issues if i.severity == "warning"]

        # Summary label
        if not self._issues:
            summary_html = "<b style='color:#44FF44'>All mods passed — no compatibility issues found.</b>"
        elif errors:
            summary_html = (
                f"<b style='color:#FF4444'>{len(errors)} error(s)</b>"
                f"{f' and <b style=\"color:#FFAA00\">{len(warnings)} warning(s)</b>' if warnings else ''}"
                " found — affected mods may crash the game."
            )
        else:
            summary_html = (
                f"<b style='color:#FFAA00'>{len(warnings)} warning(s)</b> found"
                " — mods may not work correctly with the current game version."
            )

        summary = QLabel(summary_html)
        summary.setWordWrap(True)
        layout.addWidget(summary)

        if self._issues:
            # Group by mod; mods with errors first, then warnings-only, then alpha
            mod_issues: dict[int, list[ValidationIssue]] = {}
            mod_names: dict[int, str] = {}
            for issue in self._issues:
                mod_issues.setdefault(issue.mod_id, []).append(issue)
                mod_names[issue.mod_id] = issue.mod_name

            def _sort_key(mid: int) -> tuple:
                has_err = any(i.severity == "error" for i in mod_issues[mid])
                return (0 if has_err else 1, mod_names[mid].lower())

            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll_widget = QWidget()
            scroll_layout = QVBoxLayout(scroll_widget)

            for mid in sorted(mod_issues, key=_sort_key):
                # Mod header
                n = len(mod_issues[mid])
                mod_header = QLabel(
                    f"<b>{mod_names[mid]}</b>"
                    f"<span style='color:#888'> — {n} issue{'s' if n != 1 else ''}</span>"
                )
                mod_header.setStyleSheet(
                    "QLabel { background: #2A2A2A; padding: 4px 8px; "
                    "margin-top: 8px; border-radius: 2px; }"
                )
                scroll_layout.addWidget(mod_header)

                for issue in mod_issues[mid]:
                    scroll_layout.addWidget(self._create_issue_widget(issue))

            scroll_layout.addStretch()
            scroll.setWidget(scroll_widget)
            layout.addWidget(scroll)

        # Button row
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        if self._issues:
            disable_btn = QPushButton("Disable Affected Mods")
            disable_btn.setToolTip(
                "Disable all mods that have issues and return to the main window"
            )
            disable_btn.clicked.connect(self._on_disable_affected)
            btn_layout.addWidget(disable_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_layout.addWidget(close_btn)

        layout.addLayout(btn_layout)

    def _create_issue_widget(self, issue: ValidationIssue) -> QWidget:
        widget = QWidget()
        wlayout = QVBoxLayout(widget)
        wlayout.setContentsMargins(8, 4, 8, 4)

        color = SEVERITY_COLORS.get(issue.severity, "#FFFFFF")
        label = SEVERITY_LABELS.get(issue.severity, issue.severity.upper())

        header_parts = [
            f"<span style='color:{color}; font-weight:bold'>[{label}]</span> "
            f"<b>{issue.code}: {issue.check_name}</b>",
        ]
        if issue.entry_path:
            header_parts.append(
                f"<br><span style='color:#888'>Entry: {issue.entry_path}</span>"
            )
        header = QLabel("".join(header_parts))
        header.setWordWrap(True)
        wlayout.addWidget(header)

        desc = QLabel(issue.description)
        desc.setWordWrap(True)
        desc.setStyleSheet("color: #CCC; margin-left: 16px;")
        wlayout.addWidget(desc)

        if issue.technical_detail:
            detail = QLabel(issue.technical_detail)
            detail.setWordWrap(True)
            detail.setStyleSheet(
                "color: #888; font-size: 11px; margin-left: 16px; font-family: monospace;"
            )
            wlayout.addWidget(detail)

        widget.setStyleSheet(
            f"QWidget {{ border-left: 3px solid {color}; "
            "margin-bottom: 4px; padding: 4px; }"
        )
        return widget

    def _on_disable_affected(self) -> None:
        self._should_disable = True
        self.accept()

    @property
    def should_disable(self) -> bool:
        return self._should_disable

    @property
    def affected_mod_ids(self) -> set[int]:
        return {issue.mod_id for issue in self._issues}
