import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.widgets import RectangleSelector
from datetime import timedelta, datetime
import json, os


class PlotManager:
    def __init__(self, output_frame, on_select=None, on_key=None, max_points=5000):
        self.output_frame = output_frame
        self.fig = None
        self.ax = None
        self.canvas = None
        self.selector = None

        # Data store
        self.current_df = pd.DataFrame()
        self.current_columns = []
        self.line_colors = {}
        self.lines = {}  # col -> Line2D

        # Crosshair + tooltip
        self.vline = None
        self._tooltip = None

        # X time caches (for quick search)
        self._x_pd = None                    # pandas datetime Series (tz-naive)
        self._x_np = None                    # numpy datetime64[ns] array (sorted)
        self._x_nums = None                  # matplotlib float date numbers (optional)
        self._ds_idx = None                  # downsample indices for plotting

        # Store "home view"
        self.home_xlim = None

        # Hooks
        self.on_select_hook = on_select
        self.on_key_hook = on_key

        # Cache files
        self.cache_file = "plot_cache.parquet"
        self.meta_file = "plot_cache_meta.json"

        # Perf knobs
        self.max_points = int(max_points)    # hard cap on points per line

    # -------------------------------
    # Init plot
    # -------------------------------
    def init_plot(self):
        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.output_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

        # Rectangle selector for zoom
        self.selector = RectangleSelector(
            self.ax, self._on_select,
            useblit=True, interactive=False,
            drag_from_anywhere=True, button=[1]
        )

        # Events
        self.fig.canvas.mpl_connect("motion_notify_event", self._on_mouse_motion)
        self.fig.canvas.mpl_connect("key_press_event", self._on_key)
        self.fig.canvas.mpl_connect("scroll_event", self._on_scroll)
        self.fig.canvas.mpl_connect("button_press_event", self._on_mouse_press)
        self.fig.canvas.mpl_connect("button_release_event", self._on_mouse_release)

        # Minor perf tweaks
        try:
            self.ax.set_title("Device Metrics")
            self.ax.set_xlabel("Updated at (Los Angeles)")
            self.ax.set_ylabel("Values")
            self.ax.margins(x=0)
        except Exception:
            pass

        # Date formatting
        locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
        formatter = mdates.DateFormatter("%Y-%m-%d\n%H:%M:%S")
        self.ax.xaxis.set_major_locator(locator)
        self.ax.xaxis.set_major_formatter(formatter)
        self.fig.autofmt_xdate(rotation=0, ha="center")

    # -------------------------------
    # Plot data (fast)
    # -------------------------------
    def plot_data(self, df, selected_columns, fresh=False, color_map=None):
        """If fresh=True, rebuild lines from new df.
           If fresh=False, just toggle visibility according to selected_columns.
        """
        if df is None:
            return

        # Ensure plot exists
        if self.fig is None or self.ax is None or self.canvas is None:
            self.init_plot()

        # --- Prep dataframe once on fresh datasets ---
        if fresh:
            # Reset state
            self.lines.clear()
            self.vline = None
            self._tooltip = None
            self.ax.clear()  # full wipe only on fresh/new data

            # Ensure datetime column & tz-naive
            if "updated_at" not in df.columns:
                self.ax.text(
                    0.5, 0.5, "No results",
                    ha="center", va="center", transform=self.ax.transAxes,
                    fontsize=14, color="red"
                )
                self.canvas.draw_idle()
                self.current_df = pd.DataFrame()  # reset state
                return

            # Only parse if not already datetime64[ns]
            if not pd.api.types.is_datetime64_any_dtype(df["updated_at"]):
                df = df.copy()
                df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

            # Drop bad rows and sort by time for fast searchsorted
            df = df.dropna(subset=["updated_at"]).sort_values("updated_at")

            self.current_df = df
            self._x_pd = df["updated_at"]

            # If timezone-aware, make naive
            if getattr(self._x_pd.dt, "tz", None) is not None:
                self._x_pd = self._x_pd.dt.tz_localize(None)
                self.current_df["updated_at"] = self._x_pd

            # Sanitize: ensure datetime, drop invalids
            self._x_pd = pd.to_datetime(self._x_pd, errors="coerce")
            self.current_df = self.current_df.assign(updated_at=self._x_pd)

            # Cache numpy version (aligned with sanitized series)
            self._x_np = self._x_pd.values.astype("datetime64[ns]")

            # Precompute downsample indices (shared by all series)
            n = len(self._x_pd)
            if n > self.max_points:
                step = int(np.ceil(n / self.max_points))
                self._ds_idx = np.arange(0, n, step, dtype=int)
            else:
                self._ds_idx = None

            # Axes labels & ticks again after clear
            self.ax.set_title("Device Metrics")
            self.ax.set_xlabel("Updated at (Los Angeles)")
            self.ax.set_ylabel("Values")
            locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
            formatter = mdates.DateFormatter("%Y-%m-%d\n%H:%M:%S")
            self.ax.xaxis.set_major_locator(locator)
            self.ax.xaxis.set_major_formatter(formatter)
            self.fig.autofmt_xdate(rotation=0, ha="center")

            # Home limits
            if len(self._x_pd) > 0:
                self.ax.set_xlim(self._x_pd.iloc[0], self._x_pd.iloc[-1])
                self.home_xlim = self.ax.get_xlim()

        # Keep track of selection
        self.current_columns = list(selected_columns or [])

        # --- Create or toggle lines without clearing ---
        # Build or ensure numeric columns exist
        df = self.current_df
        if df.empty:
            self.ax.text(
                0.5, 0.5, "No results",
                ha="center", va="center", transform=self.ax.transAxes,
                fontsize=14, color="red"
            )
            self.canvas.draw_idle()
            return

        # Precompute x to plot (downsampled or full)
        if self._ds_idx is not None:
            x_plot = self._x_pd.iloc[self._ds_idx]
        else:
            x_plot = self._x_pd

        # Show/hide columns by toggling visibility or creating missing lines
        selected = set(self.current_columns)
        existing = set(self.lines.keys())

        # Create lines for new selections
        for col in selected - existing:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                series = pd.to_numeric(df[col], errors="coerce")
                sub = pd.DataFrame({"updated_at": self._x_pd, col: series})

                if sub.empty:
                    continue

                # Apply downsampling consistently
                if len(sub) > self.max_points:
                    step = int(np.ceil(len(sub) / self.max_points))
                    sub = sub.iloc[::step]

                line, = self.ax.plot(
                    sub["updated_at"], sub[col],
                    label=col,
                    color=(color_map.get(col) if color_map and col in color_map else None),
                )
                self.lines[col] = line
                self.line_colors[col] = line.get_color()

        # Toggle visibility for all lines
        for col, line in self.lines.items():
            line.set_visible(col in selected)

        # Only autoscale when loading fresh data (not on every toggle)
        if fresh:
            self.ax.relim(visible_only=True)
            self.ax.autoscale(axis="y", tight=False)

        # Rebuild legend from visible lines only
        self._draw_fixed_legend()

        self.canvas.draw_idle()

    # -------------------------------
    # Tooltip + Crosshair (fast nearest)
    # -------------------------------
    def _on_mouse_motion(self, event):
        if getattr(self, "_is_panning", False):
            self._on_mouse_drag(event)
        else:
            self._on_mouse_move(event)

    def _on_mouse_move(self, event):
        if event.inaxes != self.ax or event.xdata is None or self.current_df.empty:
            return

        # Fast nearest: binary search in sorted _x_np
        try:
            mouse_dt_py = mdates.num2date(event.xdata).replace(tzinfo=None)
            mouse_ns = np.datetime64(mouse_dt_py, "ns")
            idx = int(np.searchsorted(self._x_np, mouse_ns))
            if idx <= 0:
                nearest = 0
            elif idx >= len(self._x_np):
                nearest = len(self._x_np) - 1
            else:
                prev_diff = abs(self._x_np[idx - 1] - mouse_ns)
                next_diff = abs(self._x_np[idx] - mouse_ns)
                nearest = idx - 1 if prev_diff <= next_diff else idx
            row = self.current_df.iloc[nearest]
        except Exception:
            return

        # Clean up old tooltip annotations
        if hasattr(self, "_tooltip_items"):
            for item in self._tooltip_items:
                try:
                    item.remove()
                except Exception:
                    pass
        self._tooltip_items = []

        # First line = timestamp (white)
        lines = [row["updated_at"].strftime("%Y-%m-%d %H:%M:%S")]
        colors = ["white"]

        # Add metric values with their line colors
        for col in self.current_columns:
            if col in row and pd.notna(row[col]):
                try:
                    val_str = f"{float(row[col]):.2f} : {col}"
                except Exception:
                    val_str = f"{row[col]} : {col}"
                lines.append(val_str)
                colors.append(self.line_colors.get(col, "white"))

        if not lines:
            return

        # Background box (no visible text, just black rounded pad)
        bg = self.ax.annotate(
            "\n".join(lines),
            xy=(event.xdata, event.ydata),
            xytext=(10, 10),
            textcoords="offset points",
            fontsize=9,
            color="none",
            va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.8", fc="black", alpha=0.85),
            zorder=200,
        )
        self._tooltip_items.append(bg)

        # Overlay each line with its color
        line_height = 12
        y_offset_start = 12
        for i, (text, c) in enumerate(zip(lines, colors)):
            t = self.ax.annotate(
                text,
                xy=(event.xdata, event.ydata),
                xytext=(14, y_offset_start - i * line_height),
                textcoords="offset points",
                fontsize=9,
                color=c,
                va="top", ha="left",
                zorder=201,
            )
            self._tooltip_items.append(t)

        # Vertical crosshair
        if self.vline is None:
            self.vline = self.ax.axvline(event.xdata, color="gray", linestyle="--")
        else:
            self.vline.set_xdata([event.xdata, event.xdata])

        self.canvas.draw_idle()

    # -------------------------------
    # Legend (visible-only)
    # -------------------------------
    def _draw_fixed_legend(self):
        vis_lines = [ln for ln in self.ax.get_lines() if ln.get_visible()]
        if vis_lines:
            labels = [ln.get_label() for ln in vis_lines]
            self.ax.legend(
                vis_lines, labels,
                loc="center left", bbox_to_anchor=(1.01, 0.5),
                frameon=True, fontsize=9
            )
            self.fig.subplots_adjust(right=0.8)
        else:
            leg = self.ax.get_legend()
            if leg:
                leg.remove()
            self.fig.subplots_adjust(right=0.95)

    # -------------------------------
    # Panning
    # -------------------------------
    def _on_mouse_press(self, event):
        if event.button == 3 and event.inaxes == self.ax:
            self._is_panning = True
            self._pan_start_px = (event.x, event.y)
            self._orig_xlim = self.ax.get_xlim()
            self._orig_ylim = self.ax.get_ylim()

    def _on_mouse_release(self, event):
        if event.button == 3:
            self._is_panning = False

    def _on_mouse_drag(self, event):
        if not getattr(self, "_is_panning", False) or event.inaxes != self.ax:
            return
        dx_px = event.x - self._pan_start_px[0]
        dy_px = event.y - self._pan_start_px[1]
        inv = self.ax.transData.inverted()
        x0, y0 = inv.transform((0, 0))
        x1, y1 = inv.transform((dx_px, dy_px))
        dx_data, dy_data = x1 - x0, y1 - y0
        self.ax.set_xlim(self._orig_xlim[0] - dx_data, self._orig_xlim[1] - dx_data)
        self.ax.set_ylim(self._orig_ylim[0] - dy_data, self._orig_ylim[1] - dy_data)
        self.canvas.draw_idle()

    # -------------------------------
    # Zoom & Keys
    # -------------------------------
    def _on_select(self, eclick, erelease):
        if eclick.xdata is None or erelease.xdata is None:
            return
        self.ax.set_xlim(min(eclick.xdata, erelease.xdata), max(eclick.xdata, erelease.xdata))
        self.ax.set_ylim(min(eclick.ydata, erelease.ydata), max(eclick.ydata, erelease.ydata))
        self.canvas.draw_idle()
        if self.on_select_hook:
            self.on_select_hook(eclick, erelease)

    def _on_scroll(self, event):
        if event.inaxes != self.ax:
            return
        xlim, ylim = self.ax.get_xlim(), self.ax.get_ylim()
        xdata, ydata = event.xdata, event.ydata
        scale = 1.2 if event.button == "up" else 1 / 1.2
        new_xlim = [xdata - (xdata - xlim[0]) * scale, xdata + (xlim[1] - xdata) * scale]
        new_ylim = [ydata - (ydata - ylim[0]) * scale, ydata + (ylim[1] - ydata) * scale]
        self.ax.set_xlim(new_xlim)
        self.ax.set_ylim(new_ylim)
        self.canvas.draw_idle()

    def _on_key(self, event):
        if event.key in ["escape", "r"]:
            self.reset_view()
            return

        xlim = self.ax.get_xlim()
        x0, x1 = mdates.num2date(xlim[0]), mdates.num2date(xlim[1])
        delta = timedelta(minutes=1)
        key = event.key.lower()
        if key.startswith("shift"):
            delta = timedelta(hours=1)
        if key.startswith("ctrl"):
            delta = timedelta(days=1)

        if key.endswith("left"):
            self.ax.set_xlim(x0 - delta, x1 - delta)
        elif key.endswith("right"):
            self.ax.set_xlim(x0 + delta, x1 + delta)
        self.canvas.draw_idle()

        if self.on_key_hook:
            self.on_key_hook(event)

    # -------------------------------
    # Reset view
    # -------------------------------
    def reset_view(self):
        if self.home_xlim:
            self.ax.set_xlim(self.home_xlim)
            self.ax.relim(visible_only=True)
            self.ax.autoscale(axis="y")
            self.canvas.draw_idle()

    def set_time_window(self, start_la: datetime, end_la: datetime):
        """Force the x-axis to the exact LA-naive datetime window (matches UI inputs)."""
        if self.ax is None or start_la is None or end_la is None:
            return
        if end_la <= start_la:
            return
        self.ax.set_xlim(start_la, end_la)
        # make this the "home" view for Esc / 'r'
        self.home_xlim = self.ax.get_xlim()
        if self.canvas:
            self.canvas.draw_idle()

    # -------------------------------
    # Cache
    # -------------------------------
    def _save_cache(self, df, selected_columns, col_states=None):
        try:
            cols = [c for c in (selected_columns or []) if c in df.columns]
            cols = list(dict.fromkeys(cols + ["updated_at"]))  # unique + keep order
            df[cols].to_parquet(self.cache_file)
            meta = {
                "columns": selected_columns or [],
                "col_states": col_states or {},
                "xlim": list(self.ax.get_xlim()),
                "ylim": list(self.ax.get_ylim()),
                "version": 1
            }
            with open(self.meta_file, "w") as f:
                json.dump(meta, f)
        except Exception as e:
            print(f"[Cache] Failed to save: {e}")

    def load_cache(self):
        if os.path.exists(self.cache_file) and os.path.exists(self.meta_file):
            try:
                df = pd.read_parquet(self.cache_file)
                with open(self.meta_file, "r") as f:
                    meta = json.load(f)
                columns = meta.get("columns", [])
                col_states = meta.get("col_states", {})
                xlim, ylim = meta.get("xlim"), meta.get("ylim")

                # Fresh because df changed
                self.plot_data(df, columns, fresh=True)
                if xlim: self.ax.set_xlim(xlim)
                if ylim: self.ax.set_ylim(ylim)
                self.canvas.draw_idle()

                return df, columns, col_states
            except Exception as e:
                print(f"[Cache] Failed to load: {e}")
        return None, [], {}

    def load_col_states(self):
        """Return only the cached checkbox states from meta file."""
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, "r") as f:
                    meta = json.load(f)
                return meta.get("col_states", {})
            except Exception as e:
                print(f"[Cache] Failed to load col_states: {e}")
        return {}
