from flask import Blueprint, render_template, request
from pipeline.config import TARGET_CITIES

bp = Blueprint("main", __name__)


WINDOWS = {
    "12m": ("Last 12 months", "11 months"),
    "3y":  ("Last 3 years",   "35 months"),
    "5y":  ("Last 5 years",   "59 months"),
    "all": ("All time (2007\u2013present)", None),
}

# 5y is the default — matches what the export script materializes as
# the preselected variant across most tabs.
DEFAULT_WINDOW = "5y"


def _city() -> str | None:
    c = (request.args.get("city") or "").strip().upper()
    return c if c in TARGET_CITIES else None


def _window() -> str:
    w = (request.args.get("window") or "").strip().lower()
    return w if w in WINDOWS else DEFAULT_WINDOW


def _render(template: str, active: str):
    return render_template(
        template,
        active=active,
        cities=TARGET_CITIES,
        selected_city=_city() or "ALL",
        windows=WINDOWS,
        selected_window=_window(),
    )


@bp.route("/")
def overview():
    return _render("overview.html", "overview")


@bp.route("/revenue")
def revenue():
    return _render("revenue.html", "revenue")


@bp.route("/inspections")
def inspections():
    return _render("inspections.html", "inspections")


@bp.route("/correlation")
def correlation():
    return _render("correlation.html", "correlation")


@bp.route("/map")
def map_view():
    return _render("map.html", "map")


@bp.route("/ops")
def ops():
    return _render("ops.html", "ops")


@bp.route("/establishments")
def establishments():
    return _render("establishments.html", "establishments")


@bp.route("/establishment/<int:est_id>")
def establishment(est_id: int):
    return _render("establishment.html", "")
